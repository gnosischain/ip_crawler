import os
import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import logging

# Set up logger
logger = logging.getLogger('partition_tracker')

# Ensure the logger is configured
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

class PartitionTracker:
    """
    Tracks which time partitions have been processed to avoid memory issues
    with large tables by processing them incrementally.
    """
    def __init__(self, state_file_path: str = "partition_state.json", single_run_mode: bool = False):
        """
        Initialize the partition tracker.
        
        Args:
            state_file_path: Path to the state file that persists tracking info
            single_run_mode: If True, disable reprocessing logic for one-time jobs
        """
        self.state_file_path = state_file_path
        self.single_run_mode = single_run_mode  # NEW: Track if we're in single-run mode
        
        # Define default fork digests that we're interested in
        self.fork_digests = [
            '0x56fdb5e0', '0x824be431', '0x21a6f836', 
            '0x3ebfd484', '0x7d5aab40', '0xf9ab5f85'
        ]
        
        # Load state after defining fork_digests
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file or create default if not exists."""
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    state = json.load(f)
                    # Update fork_digests from the state file if available
                    if "fork_digests" in state:
                        self.fork_digests = state["fork_digests"]
                    # Migrate from old offset-based state if needed
                    if "current_offset" in state and "last_processed_ip" not in state:
                        state["last_processed_ip"] = None
                        state.pop("current_offset", None)
                    return state
            except Exception as e:
                logger.error(f"Error loading state file: {e}")
                return self._create_default_state()
        else:
            return self._create_default_state()
    
    def _create_default_state(self) -> Dict[str, Any]:
        """Create default state structure."""
        # Default to starting from 3 months ago
        start_date = (datetime.now() - timedelta(days=90)).replace(day=1)
        
        return {
            "last_processed_month": start_date.strftime("%Y-%m-01"),
            "current_month": None,
            "last_processed_ip": None,
            "is_complete": False,
            "fork_digests": self.fork_digests
        }
    
    def save_state(self) -> None:
        """Save current state to file."""
        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(self.state, f, indent=2)
            logger.info(f"Saved partition state to {self.state_file_path}")
        except Exception as e:
            logger.error(f"Error saving state file: {e}")
    
    def get_next_partition_query(self) -> Optional[str]:
        """
        Get the query for the next partition to process.
        
        Returns:
            SQL query string or None if all partitions are processed
        """
        # If we're in the middle of a month and it's not complete
        if self.state["current_month"] and not self.state["is_complete"]:
            month_start = self.state["current_month"]
            
            # Format as list for SQL IN clause
            fork_digests_sql = ", ".join(f"'{digest}'" for digest in self.fork_digests)
            
            return f"""
            SELECT DISTINCT
                peer_properties.ip.:String AS ip
            FROM nebula.visits
            PREWHERE
                toStartOfMonth(visit_started_at) = toDate('{month_start}')
                AND (
                    peer_properties.fork_digest IN ({fork_digests_sql})
                    OR peer_properties.next_fork_version.:String LIKE '%064%'
                )
            WHERE ip != ''
            LIMIT {{batch_size}}
            """
        
        # CRITICAL FIX: In single-run mode, don't do any reprocessing logic
        if self.single_run_mode:
            logger.info("Single-run mode: Finding next unprocessed partition")
            
            # Find the next month to process
            last_processed = datetime.strptime(self.state["last_processed_month"], "%Y-%m-01")
            next_month = (last_processed + timedelta(days=32)).replace(day=1)
            
            # Don't process future months
            current_month_start = datetime.now().replace(day=1)
            if next_month > current_month_start:
                logger.info("Single-run mode: All months up to current month have been processed")
                return None
            
            # Set current month being processed
            self.state["current_month"] = next_month.strftime("%Y-%m-01")
            self.state["last_processed_ip"] = None
            self.state["is_complete"] = False
            self.save_state()
            
            logger.info(f"Single-run mode: Processing month {self.state['current_month']}")
            
            # Format as list for SQL IN clause
            fork_digests_sql = ", ".join(f"'{digest}'" for digest in self.fork_digests)
            
            return f"""
            SELECT DISTINCT
                peer_properties.ip.:String AS ip
            FROM nebula.visits
            PREWHERE
                toStartOfMonth(visit_started_at) = toDate('{self.state["current_month"]}')
                AND (
                    peer_properties.fork_digest IN ({fork_digests_sql})
                    OR peer_properties.next_fork_version.:String LIKE '%064%'
                )
            WHERE ip != ''
            LIMIT {{batch_size}}
            """
        
        # CONTINUOUS MODE LOGIC (original logic with reprocessing)
        # Find the next month to process
        last_processed = datetime.strptime(self.state["last_processed_month"], "%Y-%m-01")
        next_month = (last_processed + timedelta(days=32)).replace(day=1)
        
        # Get current and previous month for edge case handling
        now = datetime.now()
        current_month_start = now.replace(day=1)
        previous_month_start = (current_month_start - timedelta(days=1)).replace(day=1)
        
        # Handle month-end edge case - but with limits
        if now.day <= 3:  # First 3 days of the month
            logger.info(f"Early in month (day {now.day}), checking if previous month needs reprocessing")
            
            prev_month_str = previous_month_start.strftime("%Y-%m-01")
            if self.state["last_processed_month"] == prev_month_str:
                logger.info(f"Reprocessing previous month {prev_month_str} for late-arriving data")
                next_month = previous_month_start
        
        # Don't process future months, but handle current month logic
        if next_month > current_month_start:
            # Check if we should process current month
            current_month_str = current_month_start.strftime("%Y-%m-01")
            
            if self.state["last_processed_month"] != current_month_str:
                # Current month hasn't been processed yet
                logger.info(f"Starting to process current month: {current_month_str}")
                next_month = current_month_start
            else:
                # Allow reprocessing current month for new data
                logger.info(f"Reprocessing current month {current_month_str} for new data")
                next_month = current_month_start
        
        # Special case: if we're trying to process a future month, stop
        if next_month > current_month_start:
            logger.info("All available months have been processed")
            return None
        
        # Set current month being processed
        self.state["current_month"] = next_month.strftime("%Y-%m-01")
        self.state["last_processed_ip"] = None  # Reset for new partition
        self.state["is_complete"] = False
        self.save_state()
        
        # Format as list for SQL IN clause
        fork_digests_sql = ", ".join(f"'{digest}'" for digest in self.fork_digests)
        
        return f"""
        SELECT DISTINCT
            peer_properties.ip.:String AS ip
        FROM nebula.visits
        PREWHERE
            toStartOfMonth(visit_started_at) = toDate('{self.state["current_month"]}')
            AND (
                peer_properties.fork_digest IN ({fork_digests_sql})
                OR peer_properties.next_fork_version.:String LIKE '%064%'
            )
        WHERE ip != ''
        LIMIT {{batch_size}}
        """
    
    def mark_current_complete(self) -> None:
        """Mark the current partition as completely processed."""
        if self.state["current_month"]:
            # In single-run mode, always mark as fully complete
            if self.single_run_mode:
                self.state["last_processed_month"] = self.state["current_month"]
                logger.info(f"Single-run mode: Marked month {self.state['current_month']} as fully complete")
            else:
                # Continuous mode logic with delayed completion
                now = datetime.now()
                current_processing_month = datetime.strptime(self.state["current_month"], "%Y-%m-01")
                current_month_start = now.replace(day=1)
                
                if current_processing_month < current_month_start or now.day > 3:
                    self.state["last_processed_month"] = self.state["current_month"]
                    logger.info(f"Marked month {self.state['current_month']} as fully complete")
                else:
                    logger.info(f"Month {self.state['current_month']} exhausted but keeping open for reprocessing")
            
            self.state["current_month"] = None
            self.state["last_processed_ip"] = None  # Reset last IP
            self.state["is_complete"] = True
            self.save_state()
    
    def update_last_processed_ip(self, ip: str) -> None:
        """
        Update the last processed IP for the current partition.
        
        Args:
            ip: The last IP that was processed
        """
        self.state["last_processed_ip"] = ip
        self.save_state()
        logger.debug(f"Updated last processed IP to: {ip}")
    
    def update_fork_digests(self, new_digests: List[str]) -> None:
        """
        Update the list of fork digests to track.
        
        Args:
            new_digests: New list of fork digests
        """
        self.fork_digests = new_digests
        self.state["fork_digests"] = new_digests
        self.save_state()
        logger.info(f"Updated fork digests to: {new_digests}")