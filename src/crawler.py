import os
import time
import logging
import requests
import json
import argparse
from typing import Dict, List, Optional, Any
from datetime import datetime
import signal
import sys

# For API rate limiting
from ratelimit import limits, sleep_and_retry
from backoff import on_exception, expo
from requests.exceptions import RequestException, Timeout

from src.config import (
    IPINFO_API_TOKEN, BATCH_SIZE, SLEEP_INTERVAL, 
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY, RATE_LIMIT_SECONDS,
    LOG_PATH, IP_SOURCE_QUERIES
)
from src.db import Database
from src.utils import sanitize_ip_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_PATH, 'crawler.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ip_crawler')

# Touch health check file
with open(os.path.join(LOG_PATH, 'health.log'), 'w') as f:
    f.write(f"Crawler started at {datetime.now().isoformat()}")

# Default fork digests to track
DEFAULT_FORK_DIGESTS = [
    '0x56fdb5e0', '0x824be431', '0x21a6f836', 
    '0x3ebfd484', '0x7d5aab40', '0xf9ab5f85'
]

class IPInfoCrawler:
    def __init__(self, single_run_mode=False):
        # Pass single_run_mode to the database
        self.db = Database(single_run_mode=single_run_mode)
        self.running = True
        self.single_run_mode = single_run_mode
        self.setup_signal_handlers()
        
        # Load fork digests from environment or use defaults
        fork_digests_env = os.environ.get('FORK_DIGESTS', '')
        if fork_digests_env and fork_digests_env.strip():
            self.fork_digests = [d.strip() for d in fork_digests_env.split(',') if d.strip()]
        else:
            self.fork_digests = DEFAULT_FORK_DIGESTS.copy()
        
        # Update the tracker with our initial fork digests if they're different
        tracker_digests = self.db.tracker.fork_digests
        if sorted(self.fork_digests) != sorted(tracker_digests):
            self.db.update_fork_digests(self.fork_digests)
        
        mode_text = "single-run" if single_run_mode else "continuous"
        logger.info(f"IP Info Crawler initialized in {mode_text} mode with fork digests: {self.fork_digests}")
        
        # Log rate limit settings
        if RATE_LIMIT_SECONDS > 0:
            logger.info(f"Rate limit set to 1 request per {RATE_LIMIT_SECONDS:.2f} seconds")
        else:
            logger.info("Rate limiting disabled")

    def setup_signal_handlers(self):
        """Set up handlers for graceful shutdown."""
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, self.handle_shutdown)
    
    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    @sleep_and_retry
    @limits(calls=1, period=RATE_LIMIT_SECONDS)
    @on_exception(expo, RequestException, max_tries=MAX_RETRIES, max_time=30)
    def fetch_ip_info(self, ip: str) -> Dict[str, Any]:
        """
        Fetch IP information from ipinfo.io API with rate limiting and retries.
        
        This method is decorated with:
        - sleep_and_retry and limits to enforce rate limiting
        - on_exception for exponential backoff retries
        """
        logger.debug(f"Fetching info for IP: {ip}")
        headers = {"Authorization": f"Bearer {IPINFO_API_TOKEN}"}
        
        response = requests.get(
            f"https://ipinfo.io/{ip}", 
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:  # Rate limit exceeded
            retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
            logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds.")
            time.sleep(retry_after)
            raise RequestException("Rate limit exceeded")
        else:
            logger.error(f"Failed to fetch info for IP {ip}: {response.status_code} - {response.text}")
            raise RequestException(f"API error: {response.status_code}")

    def process_ip(self, ip: str) -> bool:
        """Process a single IP address."""
        try:
            # Skip if IP already exists in the database
            if self.db.check_ip_exists(ip):
                logger.debug(f"IP {ip} already exists in database, skipping")
                return True
            
            # Fetch information from ipinfo.io
            ip_info = self.fetch_ip_info(ip)
            
            # Sanitize the data before saving
            sanitized_info = sanitize_ip_info(ip_info)
            
            # Save the information to database
            self.db.save_ip_info(sanitized_info)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing IP {ip}: {str(e)}")
            
            # Save error information
            error_info = {
                'ip': ip,
                'error': str(e)
            }
            self.db.save_ip_info(error_info, success=False, error=str(e))
            
            return False

    def run_ip_source(self, query: str, source_label: str = 'custom') -> Dict[str, Any]:
        """Enrich an explicit list of IPs from a SELECT, bypassing PartitionTracker.

        Why this exists: the default crawl discovers IPs by walking nebula.visits
        month by month, filtered on beacon-chain fork digests. Other datasets (the
        first being HOPR mixnet nodes, whose IPs are announced on-chain and
        extracted in dbt.int_hopr_nodes) have their own IP list, no month
        partitioning and no fork digests, so none of that machinery applies.

        Only the DISCOVERY differs. Fetch, sanitize and insert all go through the
        same process_ip(), so enrichment stays identical no matter the source and
        the nebula path is left completely untouched.

        Always terminates: it walks a finite list once. No tracker state is read
        or written, so running it cannot disturb an in-flight nebula crawl.
        """
        logger.info(f"Running explicit IP source: {source_label}")
        stats = {
            'source': source_label, 'total_ips': 0, 'successful': 0,
            'failed': 0, 'skipped_existing': 0,
        }

        try:
            ips = self.db.get_ips_from_query(query)
        except Exception as e:
            logger.error(f"Could not resolve IP source {source_label!r}: {e}")
            stats['error'] = str(e)
            return stats

        stats['total_ips'] = len(ips)
        if not ips:
            logger.info(f"Source {source_label!r}: nothing to enrich, all IPs already stored")
            return stats

        logger.info(f"Source {source_label!r}: fetching {len(ips)} IP(s) from ipinfo.io")
        for index, ip in enumerate(ips, start=1):
            if not self.running:
                logger.info("Shutdown requested, stopping IP source run")
                break
            if self.process_ip(ip):
                stats['successful'] += 1
            else:
                stats['failed'] += 1
            if index % 100 == 0:
                logger.info(f"Source {source_label!r}: {index}/{len(ips)} processed")

        stats['success_rate'] = round(
            100 * stats['successful'] / stats['total_ips'], 2
        ) if stats['total_ips'] else 0.0
        logger.info(
            f"Source {source_label!r} complete: {stats['successful']} ok, "
            f"{stats['failed']} failed of {stats['total_ips']}"
        )
        return stats

    def run_single_batch(self) -> Dict[str, Any]:
        """Run a single batch and return statistics."""
        logger.info("Running single batch job - will process current partition completely")
        
        # Update health check file
        with open(os.path.join(LOG_PATH, 'health.log'), 'w') as f:
            f.write(f"Single batch job running at {datetime.now().isoformat()}")
        
        # Track overall statistics
        total_successful = 0
        total_failed = 0
        total_ips_processed = 0
        batches_processed = 0
        
        # Process until the current partition is exhausted
        while self.running:
            # Get batch of unprocessed IPs
            ips = self.db.get_unprocessed_ips(BATCH_SIZE)
            
            if not ips:
                # Check if partition was exhausted or if we're truly done
                if self.db.get_partition_exhausted():
                    logger.info("Current partition exhausted, single-run complete")
                    break
                else:
                    logger.info("No IPs in current batch, but partition not exhausted yet")
                    continue
            
            batches_processed += 1
            logger.info(f"Processing batch #{batches_processed} with {len(ips)} IPs")
            
            # Process each IP in the batch
            for i, ip in enumerate(ips, 1):
                if not self.running:
                    logger.info("Shutdown requested, stopping processing")
                    break
                
                logger.info(f"Processing IP {i}/{len(ips)}: {ip}")
                total_ips_processed += 1
                
                if self.process_ip(ip):
                    total_successful += 1
                else:
                    total_failed += 1
            
            logger.info(f"Batch #{batches_processed} completed: {len(ips)} IPs processed")
        
        # Get final statistics
        stats = {
            "total_ips": total_ips_processed,
            "successful": total_successful,
            "failed": total_failed,
            "batches_processed": batches_processed,
            "success_rate": round((total_successful / total_ips_processed * 100) if total_ips_processed > 0 else 0, 2)
        }
        
        if total_ips_processed == 0:
            stats["message"] = "No new IPs found to process"
        
        logger.info(f"Single batch job completed: {json.dumps(stats)}")
        
        # Get database statistics
        try:
            db_stats = self.db.get_db_stats()
            stats["database_stats"] = db_stats
            logger.info(f"Database stats: {json.dumps(db_stats)}")
        except Exception as e:
            logger.error(f"Error getting database stats: {str(e)}")
        
        return stats

    def run_crawler(self):
        """Main crawler method - handles both single-run and continuous modes."""
        if self.single_run_mode:
            logger.info("Starting IP Info crawler in single-run mode")
            stats = self.run_single_batch()
            
            # Write final statistics to a file for easy access
            stats_file = os.path.join(LOG_PATH, 'last_run_stats.json')
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2)
            
            logger.info(f"Single-run completed. Statistics saved to {stats_file}")
            return stats
        else:
            # Original continuous mode
            logger.info("Starting IP Info crawler in continuous mode")
            self._run_continuous_mode()

    def _run_continuous_mode(self):
        """Original continuous crawler loop."""
        batch_count = 0
        empty_result_count = 0
        while self.running:
            try:
                # Update health check file
                with open(os.path.join(LOG_PATH, 'health.log'), 'w') as f:
                    f.write(f"Crawler running at {datetime.now().isoformat()}")
                
                # Check for fork digest updates in environment variable
                fork_digests_env = os.environ.get('FORK_DIGESTS', '')
                if fork_digests_env and fork_digests_env.strip():
                    new_fork_digests = [d.strip() for d in fork_digests_env.split(',') if d.strip()]
                    if new_fork_digests and sorted(new_fork_digests) != sorted(self.fork_digests):
                        logger.info(f"Updating fork digests from {self.fork_digests} to {new_fork_digests}")
                        self.fork_digests = new_fork_digests
                        self.db.update_fork_digests(self.fork_digests)
                
                # Get batch of unprocessed IPs
                ips = self.db.get_unprocessed_ips(BATCH_SIZE)
                
                if not ips:
                    empty_result_count += 1
                    logger.info(f"No new IPs to process. Sleeping... (empty count: {empty_result_count})")
                    
                    # If we've had multiple empty results, wait longer
                    sleep_time = min(SLEEP_INTERVAL * (1 + empty_result_count // 5), 300)  # Max 5 minutes
                    time.sleep(sleep_time)
                    continue
                
                # Reset empty result counter when we find IPs
                empty_result_count = 0
                batch_count += 1
                logger.info(f"Processing batch #{batch_count} with {len(ips)} IPs")
                
                # Process each IP
                successful = 0
                for ip in ips:
                    if not self.running:
                        logger.info("Shutdown requested, stopping processing")
                        break
                        
                    if self.process_ip(ip):
                        successful += 1
                
                # Log batch completion
                logger.info(f"Completed batch #{batch_count}: {successful}/{len(ips)} successful")
                
                # Get and log statistics periodically
                if batch_count % 10 == 0:
                    try:
                        stats = self.db.get_db_stats()
                        logger.info(f"Database stats: {json.dumps(stats)}")
                    except Exception as e:
                        logger.error(f"Error getting database stats: {str(e)}")
                
                # Sleep between batches
                logger.info(f"Sleeping for {SLEEP_INTERVAL} seconds...")
                time.sleep(SLEEP_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in crawler loop: {str(e)}")
                logger.info(f"Sleeping for {SLEEP_INTERVAL} seconds before retry...")
                time.sleep(SLEEP_INTERVAL)
        
        logger.info("Crawler stopped")

def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(description='IP Info Crawler')
    parser.add_argument('--once', '--single-run', action='store_true', 
                       help='Run once and exit instead of continuous mode')
    parser.add_argument('--batch-size', type=int, 
                       help='Override batch size for single run')
    parser.add_argument('--source', choices=sorted(IP_SOURCE_QUERIES),
                       help='Enrich a named explicit IP source instead of crawling '
                            'nebula.visits (e.g. "hopr"). Runs once and exits.')
    parser.add_argument('--ips-query',
                       help='Enrich IPs from an ad-hoc read-only SELECT returning one '
                            'IP column. Mutually exclusive with --source.')
    
    args = parser.parse_args()
    
    if args.source and args.ips_query:
        parser.error('--source and --ips-query are mutually exclusive')
    
    # Override batch size if specified
    if args.batch_size:
        global BATCH_SIZE
        BATCH_SIZE = args.batch_size
        logger.info(f"Batch size overridden to: {BATCH_SIZE}")
    
    try:
        logger.info("Starting IP Info Crawler")

        # Explicit-source mode: finite list, runs once, never touches the
        # PartitionTracker state the nebula crawl depends on.
        if args.source or args.ips_query:
            crawler = IPInfoCrawler(single_run_mode=True)
            if args.source:
                query, label = IP_SOURCE_QUERIES[args.source], args.source
            else:
                query, label = args.ips_query, 'ad-hoc'
            result = crawler.run_ip_source(query, source_label=label)

            print()
            print("="*50)
            print(f"IP SOURCE RUN SUMMARY ({result['source']})")
            print("="*50)
            print(f"IPs needing enrichment: {result['total_ips']}")
            print(f"Successful: {result['successful']}")
            print(f"Failed: {result['failed']}")
            if 'success_rate' in result:
                print(f"Success rate: {result['success_rate']}%")
            if 'error' in result:
                print(f"ERROR: {result['error']}")
            print("="*50)

            # Exit code contract, written for a scheduled job rather than a human:
            #
            #   error                     -> 1  (could not even resolve the IP list)
            #   tried some, none worked   -> 1  (bad token, rate limit, ipinfo down)
            #   partial failures          -> 0  (see below)
            #   nothing to do             -> 0  (everything already enriched)
            #
            # Total failure has to be non-zero or a broken run looks green forever: the
            # job "succeeds" every night while enriching nothing, and the only symptom
            # is geo that quietly stops improving.
            #
            # Partial failure stays SUCCESS on purpose. A handful of IPs failing is
            # normal transient API behaviour, and it is self-healing -- those IPs are
            # still missing from ipinfo, so the next run simply retries them. Failing
            # the job would raise an alert for something that fixes itself.
            attempted = result.get('total_ips', 0)
            succeeded = result.get('successful', 0)
            total_failure = attempted > 0 and succeeded == 0
            if total_failure:
                logger.error(
                    f"IP source {result['source']!r}: all {attempted} enrichment(s) "
                    "failed - treating as job failure"
                )
            sys.exit(1 if (result.get('error') or total_failure) else 0)

        crawler = IPInfoCrawler(single_run_mode=args.once)
        result = crawler.run_crawler()
        
        if args.once:
            # Print summary for single-run mode
            print("\n" + "="*50)
            print("SINGLE RUN SUMMARY")
            print("="*50)
            print(f"Total IPs processed: {result['total_ips']}")
            print(f"Successful: {result['successful']}")
            print(f"Failed: {result['failed']}")
            print(f"Batches processed: {result['batches_processed']}")
            print(f"Success rate: {result['success_rate']}%")
            
            if 'database_stats' in result:
                db_stats = result['database_stats']
                print(f"Total in database: {db_stats['total_processed']}")
                print(f"Overall success rate: {db_stats['success_rate']}%")
            
            print("="*50)
            
    except KeyboardInterrupt:
        logger.info("Crawler stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()