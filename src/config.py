"""Configuration, read once from the environment (and a local .env if present)."""
import os

from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


# --- ClickHouse -----------------------------------------------------------------
# clickhouse-connect speaks HTTP only: 8123 plain, 8443 TLS. The old default of 9000
# (native protocol) could never connect.
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = _int("CLICKHOUSE_PORT", 8443)
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "crawlers_data")
CLICKHOUSE_SECURE = _bool("CLICKHOUSE_SECURE", "true")

# Target table (created by src/migrations.py) and the Nebula visits table it reads.
IP_INFO_TABLE = os.environ.get("IP_INFO_TABLE", "ipinfo")
NEBULA_VISITS_TABLE = os.environ.get("NEBULA_VISITS_TABLE", "nebula.visits")

# --- Discovery window -----------------------------------------------------------
# A run looks at visits in [now - LOOKBACK_DAYS, now), one ClickHouse query per
# WINDOW_CHUNK_HOURS so no single query scans more than a day, and stops collecting
# at MAX_IPS_PER_RUN (10 000 lookups at the default rate limit is about five hours,
# inside the Kubernetes Job deadline; whatever is left is found by the next run).
LOOKBACK_DAYS = _int("LOOKBACK_DAYS", 2)
WINDOW_CHUNK_HOURS = _int("WINDOW_CHUNK_HOURS", 24)
MAX_IPS_PER_RUN = _int("MAX_IPS_PER_RUN", 10000)

# Fetch and print the work list, no ipinfo calls, no inserts.
DRY_RUN = _bool("DRY_RUN")

# Beacon-chain fork digests that identify Gnosis peers in nebula.visits.
DEFAULT_FORK_DIGESTS = [
    "0x56fdb5e0", "0x824be431", "0x21a6f836",
    "0x3ebfd484", "0x7d5aab40", "0xf9ab5f85",
]
FORK_DIGESTS = [d.strip() for d in os.environ.get("FORK_DIGESTS", "").split(",") if d.strip()] or list(DEFAULT_FORK_DIGESTS)

# --- Explicit IP sources (`--source` / `--ips-query`) ----------------------------
# A source is any read-only SELECT that yields a column named `ip`. It goes through
# the same work-list wrapper as the nebula window (anti-join against the ipinfo
# table, cap), so a source is never asked for rows the crawler would discard.
HOPR_NODES_TABLE = os.environ.get("HOPR_NODES_TABLE", "dbt.int_hopr_nodes")

IP_SOURCE_QUERIES = {
    # HOPR mixnet / GnosisVPN nodes. `announced_ip` is the IPv4 a node published
    # on-chain via HoprAnnouncements.AddressAnnouncement, extracted in dbt.
    # Enriching these turns int_hopr_nodes.geo_source from 'unenriched' to
    # 'ipinfo' with no dbt model change -- that model already LEFT JOINs ipinfo.
    "hopr": f"""
        SELECT DISTINCT announced_ip AS ip
        FROM {HOPR_NODES_TABLE}
        WHERE announced_ip IS NOT NULL AND announced_ip != ''
    """,
}

# --- ipinfo.io ------------------------------------------------------------------
IPINFO_API_TOKEN = os.environ.get("IPINFO_API_TOKEN", "")
IPINFO_RATE_LIMIT = _int("IPINFO_RATE_LIMIT", 50000)  # requests per day; 0 disables throttling
# Seconds between requests to stay inside the daily budget with a 5% margin.
RATE_LIMIT_SECONDS = 86400 / (IPINFO_RATE_LIMIT * 0.95) if IPINFO_RATE_LIMIT > 0 else 0.0
REQUEST_TIMEOUT = _int("REQUEST_TIMEOUT", 10)   # seconds per HTTP request
MAX_RETRIES = _int("MAX_RETRIES", 3)            # attempts per IP on transient failures
RETRY_DELAY = _int("RETRY_DELAY", 5)            # base seconds between those attempts

# --- Run behaviour --------------------------------------------------------------
BATCH_SIZE = _int("BATCH_SIZE", 50)             # progress-log / health-file cadence, in IPs
SLEEP_INTERVAL = _int("SLEEP_INTERVAL", 3600)   # continuous mode: pause between window runs

# --- Paths ----------------------------------------------------------------------
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATIONS_PATH = os.path.join(_ROOT, "migrations")
LOG_PATH = os.environ.get("CRAWLER_LOG_PATH", os.path.join(_ROOT, "logs"))
os.makedirs(LOG_PATH, exist_ok=True)
