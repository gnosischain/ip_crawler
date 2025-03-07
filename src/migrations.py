import os
import logging
import time
from typing import List
import glob

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from src.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, 
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, CLICKHOUSE_SECURE,
    MIGRATIONS_PATH
)

# Set up logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('migrations')

def run_migrations() -> None:
    """Run all migration files in the migrations directory."""
    logger.info("Starting database migrations")
    
    # Try to connect with retries
    client = connect_with_retry()
    
    # First, try to create the database if it doesn't exist
    try:
        logger.info(f"Creating database {CLICKHOUSE_DATABASE} if it doesn't exist")
        client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
    except Exception as e:
        logger.error(f"Error creating database: {str(e)}")
        raise
    
    # Get list of migration files
    migration_files = get_migration_files()
    
    if not migration_files:
        logger.warning("No migration files found in %s", MIGRATIONS_PATH)
        return
    
    logger.info(f"Found {len(migration_files)} migration files")
    
    # Run each migration file
    for file_path in migration_files:
        file_name = os.path.basename(file_path)
        logger.info(f"Running migration: {file_name}")
        
        try:
            with open(file_path, 'r') as f:
                sql = f.read()
            
            # Run the migration queries
            execute_migration(client, sql, file_name)
            
            logger.info(f"Successfully applied migration: {file_name}")
            
        except Exception as e:
            logger.error(f"Error applying migration {file_name}: {str(e)}")
            raise
    
    logger.info("Migrations completed successfully")

def connect_with_retry(max_retries: int = 5, retry_delay: int = 5) -> Client:
    """Connect to ClickHouse with retry logic."""
    retries = 0
    last_error = None
    
    while retries < max_retries:
        try:
            # Connect to the server
            logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                secure=CLICKHOUSE_SECURE
            )
            # Test the connection
            client.command("SELECT 1")
            logger.info(f"Successfully connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
            return client
        except Exception as e:
            last_error = e
            retries += 1
            logger.warning(f"Connection attempt {retries} failed: {str(e)}")
            time.sleep(retry_delay)
    
    logger.error(f"Failed to connect to ClickHouse after {max_retries} attempts")
    raise last_error

def get_migration_files() -> List[str]:
    """Get sorted list of SQL migration files."""
    pattern = os.path.join(MIGRATIONS_PATH, "*.sql")
    files = glob.glob(pattern)
    return sorted(files)

def execute_migration(client: Client, sql: str, file_name: str) -> None:
    """Execute a migration SQL file."""
    # Split by semicolon to handle multiple statements
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    
    for i, statement in enumerate(statements, 1):
        try:
            # Execute the statement
            client.command(statement)
            logger.debug(f"Executed statement {i} in {file_name}")
        except Exception as e:
            logger.error(f"Error executing statement {i} in {file_name}: {str(e)}")
            logger.error(f"Statement: {statement}")
            raise

if __name__ == "__main__":
    run_migrations()