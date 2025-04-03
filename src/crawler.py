import os
import time
import logging
import requests
import json
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
    LOG_PATH
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
    def __init__(self):
        self.db = Database()
        self.running = True
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
        
        logger.info(f"IP Info Crawler initialized with fork digests: {self.fork_digests}")
        
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

    def run_crawler(self):
        """Main crawler loop."""
        logger.info("Starting IP Info crawler loop")
        
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

if __name__ == "__main__":
    try:
        logger.info("Starting IP Info Crawler")
        crawler = IPInfoCrawler()
        crawler.run_crawler()
    except KeyboardInterrupt:
        logger.info("Crawler stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        sys.exit(1)