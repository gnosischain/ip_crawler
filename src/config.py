import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ClickHouse Connection Settings
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'crawlers_data')
CLICKHOUSE_SECURE = os.environ.get('CLICKHOUSE_SECURE', 'false').lower() == 'true'

# Table Configuration
IP_INFO_TABLE = os.environ.get('IP_INFO_TABLE', 'ipinfo')

# IPInfo API Configuration
IPINFO_API_TOKEN = os.environ.get('IPINFO_API_TOKEN', '')
IPINFO_RATE_LIMIT = int(os.environ.get('IPINFO_RATE_LIMIT', 50000))  # Requests per day

# Calculate rate in seconds between requests to meet daily limit
# Add 5% buffer to be safe (24 * 60 * 60 = 86400 seconds in a day)
RATE_LIMIT_SECONDS = 86400 / (IPINFO_RATE_LIMIT * 0.95) if IPINFO_RATE_LIMIT > 0 else 0

# Crawler Settings
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))
SLEEP_INTERVAL = int(os.environ.get('SLEEP_INTERVAL', 5))  # seconds between batch processing
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', 10))  # seconds for API requests
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 3))  # Maximum number of retries for failed requests
RETRY_DELAY = int(os.environ.get('RETRY_DELAY', 5))  # seconds between retries

# Application paths
MIGRATIONS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'migrations')
LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')

# Ensure log directory exists
os.makedirs(LOG_PATH, exist_ok=True)