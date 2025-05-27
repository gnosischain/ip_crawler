import os
import logging
from typing import Dict, List, Optional, Tuple, Any, Union, Set
import time
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, 
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_SECURE,
    IP_INFO_TABLE, LOG_PATH
)
from src.partition_tracker import PartitionTracker

# Set up logger
logger = logging.getLogger('db')

class Database:
    def __init__(self):
        self.client = self._create_client()
        logger.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        
        # Initialize the partition tracker
        self.tracker = PartitionTracker(os.path.join(LOG_PATH, "partition_state.json"))
        logger.info("Initialized partition tracker")
        
        # Keep track of IPs we've seen in current partition to avoid re-querying
        self.seen_ips_in_partition: Set[str] = set()
        self.current_partition_id = None

    def _create_client(self) -> Client:
        """Create and return a ClickHouse client."""
        logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                secure=CLICKHOUSE_SECURE
            )
            # Test connection
            client.command("SELECT 1")
            logger.info("ClickHouse connection established successfully")
            return client
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(ClickHouseError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True
    )
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Tuple]:
        """Execute a query with retry logic."""
        try:
            return self.client.query(query, parameters=params).result_rows
        except ClickHouseError as e:
            logger.error(f"Database error: {str(e)}")
            # Attempt to reconnect before retry
            self.client = self._create_client()
            raise

    def execute_command(self, command: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Execute a command with no result."""
        try:
            self.client.command(command, parameters=params)
        except ClickHouseError as e:
            logger.error(f"Database command error: {str(e)}")
            raise

    def execute_file(self, file_path: str) -> None:
        """Execute SQL from a file."""
        try:
            with open(file_path, 'r') as f:
                sql = f.read()
                
            # Split by semicolon to handle multiple statements
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            for statement in statements:
                self.client.command(statement)
                
        except Exception as e:
            logger.error(f"Error executing SQL file {file_path}: {str(e)}")
            raise

    def get_unprocessed_ips(self, limit: int) -> List[str]:
        """Get IPs that haven't been processed yet using partition-based approach."""
        try:
            # Check if we're starting a new partition
            current_month = self.tracker.state.get("current_month")
            if current_month != self.current_partition_id:
                logger.info(f"Starting new partition: {current_month}")
                self.seen_ips_in_partition.clear()
                self.current_partition_id = current_month
            
            # Get the query for the current partition
            query_template = self.tracker.get_next_partition_query()
            
            if not query_template:
                logger.info("No more partitions to process at this time")
                return []
            
            # Build exclusion list for IPs we've already queried in this partition
            if self.seen_ips_in_partition:
                # For safety, limit the exclusion list size
                if len(self.seen_ips_in_partition) > 10000:
                    logger.warning("Too many IPs in exclusion list, marking partition complete")
                    self.tracker.mark_current_complete()
                    self.seen_ips_in_partition.clear()
                    self.current_partition_id = None
                    return self.get_unprocessed_ips(limit)
                
                # Add exclusion filter
                excluded_ips = "', '".join(self.seen_ips_in_partition)
                exclusion_filter = f" AND ip NOT IN ('{excluded_ips}')"
                query = query_template.replace(
                    "WHERE ip != ''",
                    f"WHERE ip != ''{exclusion_filter}"
                ).format(batch_size=limit)
            else:
                # No exclusions needed for first query
                query = query_template.format(batch_size=limit)
            
            # Execute the query
            result = self.execute(query)
            ips = [row[0] for row in result]
            
            if not ips:
                logger.info("No more IPs in current partition, marking complete")
                self.tracker.mark_current_complete()
                self.seen_ips_in_partition.clear()
                self.current_partition_id = None
                # Try the next partition
                return self.get_unprocessed_ips(limit)
            
            logger.info(f"Retrieved {len(ips)} IPs from partition")
            
            # Add these IPs to our seen set
            self.seen_ips_in_partition.update(ips)
            
            # Filter out IPs we've already processed in the database
            unprocessed_ips = []
            for ip in ips:
                if not self.check_ip_exists(ip):
                    unprocessed_ips.append(ip)
            
            logger.info(f"Found {len(unprocessed_ips)} unprocessed IPs out of {len(ips)} total")
            
            # If all IPs were already processed, continue to next batch
            if len(unprocessed_ips) == 0 and len(ips) > 0:
                logger.info("All IPs in this batch were already processed, fetching next batch...")
                return self.get_unprocessed_ips(limit)
            
            return unprocessed_ips
            
        except Exception as e:
            logger.error(f"Error getting unprocessed IPs: {e}")
            # If there's an error with the exclusion list, clear it and try again
            if "too long" in str(e).lower() or "memory" in str(e).lower():
                logger.warning("Query too complex, clearing seen IPs and retrying")
                self.seen_ips_in_partition.clear()
                return self.get_unprocessed_ips(limit)
            return []

    def save_ip_info(self, ip_info: Dict[str, Any], success: bool = True, error: str = '') -> None:
        """Save IP information to ClickHouse."""
        # Extract values with defaults for missing keys
        data = {
            'ip': ip_info.get('ip', ''),
            'hostname': ip_info.get('hostname', ''),
            'city': ip_info.get('city', ''),
            'region': ip_info.get('region', ''),
            'country': ip_info.get('country', ''),
            'loc': ip_info.get('loc', ''),
            'org': ip_info.get('org', ''),
            'postal': ip_info.get('postal', ''),
            'timezone': ip_info.get('timezone', ''),
            'asn': ip_info.get('asn', ''),
            'company': ip_info.get('company', {}).get('name', '') if isinstance(ip_info.get('company'), dict) else '',
            'carrier': ip_info.get('carrier', {}).get('name', '') if isinstance(ip_info.get('carrier'), dict) else '',
            'is_bogon': ip_info.get('bogon', False),
            'is_mobile': ip_info.get('mobile', False),
            'abuse_email': ip_info.get('abuse', {}).get('email', '') if isinstance(ip_info.get('abuse'), dict) else '',
            'abuse_phone': ip_info.get('abuse', {}).get('phone', '') if isinstance(ip_info.get('abuse'), dict) else '',
            'error': error,
            'success': success
        }
        
        # Create columns and values lists
        columns = ', '.join(f'`{k}`' for k in data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        values = list(data.values())
        
        query = f"""
        INSERT INTO {CLICKHOUSE_DATABASE}.{IP_INFO_TABLE} 
        ({columns})
        VALUES
        ({placeholders})
        """
        
        self.client.insert(f"{CLICKHOUSE_DATABASE}.{IP_INFO_TABLE}", [values], column_names=list(data.keys()))
        logger.info(f"Saved info for IP: {ip_info.get('ip')}")

    def check_ip_exists(self, ip: str) -> bool:
        """Check if an IP already exists in the ipinfo table."""
        query = f"""
        SELECT 1 FROM {CLICKHOUSE_DATABASE}.{IP_INFO_TABLE}
        WHERE ip = '{ip}'
        LIMIT 1
        """
        result = self.execute(query)
        return len(result) > 0
        
    def update_fork_digests(self, new_digests: List[str]) -> None:
        """Update the fork digests in the tracker."""
        self.tracker.update_fork_digests(new_digests)

    def get_db_stats(self) -> Dict[str, Union[int, float]]:
        """Get statistics about the database."""
        # Get total IPs processed
        total_query = f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{IP_INFO_TABLE}"
        total_processed = self.execute(total_query)[0][0]
        
        # Get successful lookups
        success_query = f"""
        SELECT count() FROM {CLICKHOUSE_DATABASE}.{IP_INFO_TABLE}
        WHERE success = true
        """
        successful_lookups = self.execute(success_query)[0][0]
        
        # Get failed lookups
        failed_query = f"""
        SELECT count() FROM {CLICKHOUSE_DATABASE}.{IP_INFO_TABLE}
        WHERE success = false
        """
        failed_lookups = self.execute(failed_query)[0][0]
        
    def get_partition_exhausted(self) -> bool:
        """Check if the current partition has been exhausted."""
        return self.current_partition_id is None or len(self.seen_ips_in_partition) == 0