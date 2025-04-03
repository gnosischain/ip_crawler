import os
import logging
from typing import Dict, List, Optional, Tuple, Any, Union
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
            # Get the query for the next partition to process
            query_template = self.tracker.get_next_partition_query()
            
            if not query_template:
                logger.info("No more partitions to process at this time")
                return []
            
            # Format the query with the batch size
            query = query_template.format(batch_size=limit)
            
            # Execute the query directly without creating a temporary table
            result = self.execute(query)
            ips = [row[0] for row in result]
            
            # If we got fewer results than the limit, this partition is complete
            if len(ips) < limit:
                logger.info(f"Partition completed with {len(ips)} IPs")
                self.tracker.mark_current_complete()
            
            # Filter out IPs we've already processed
            unprocessed_ips = []
            for ip in ips:
                if not self.check_ip_exists(ip):
                    unprocessed_ips.append(ip)
            
            logger.info(f"Found {len(unprocessed_ips)} unprocessed IPs out of {len(ips)} total")
            return unprocessed_ips
            
        except Exception as e:
            logger.error(f"Error getting unprocessed IPs: {e}")
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
        
        return {
            "total_processed": total_processed,
            "successful_lookups": successful_lookups,
            "failed_lookups": failed_lookups,
            "success_rate": round((successful_lookups / total_processed * 100) if total_processed > 0 else 0, 2)
        }