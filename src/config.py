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

# ---------------------------------------------------------------------------
# Explicit IP sources (`--source` / `--ips-query`)
#
# The default crawl walks nebula.visits month by month via PartitionTracker and
# filters on beacon-chain fork digests. That machinery is specific to the P2P
# census: other datasets have their own IP lists and no month partitioning, so
# they cannot reuse it.
#
# These presets feed the SAME enrichment path (Crawler.process_ip) from an
# arbitrary SELECT instead. The nebula path is untouched.
#
# A preset must return exactly ONE column of IPv4 strings. Rows already present
# in the ipinfo table are skipped before any API call is made.
# ---------------------------------------------------------------------------
HOPR_NODES_TABLE = os.environ.get('HOPR_NODES_TABLE', 'dbt.int_hopr_nodes')

IP_SOURCE_QUERIES = {
    # HOPR mixnet / GnosisVPN nodes. `announced_ip` is the IPv4 a node published
    # on-chain via HoprAnnouncements.AddressAnnouncement, extracted in dbt.
    # Enriching these turns int_hopr_nodes.geo_source from 'unenriched' to
    # 'ipinfo' with no dbt model change -- that model already LEFT JOINs ipinfo.
    'hopr': f"""
        SELECT DISTINCT announced_ip AS ip
        FROM {HOPR_NODES_TABLE}
        WHERE announced_ip IS NOT NULL AND announced_ip != ''
    """,
}

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