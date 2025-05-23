# IP Info Crawler

A service that continuously fetches IP address information from ipinfo.io and stores it in a ClickHouse database.

## Features

- Connects to ClickHouse Cloud
- Creates necessary database and tables if they don't exist
- Handles rate limiting for the ipinfo.io API (up to 10 requests per second)
- Implements retry logic and error handling
- Processes IPs in batches for efficiency
- Processes large tables incrementally by month to avoid memory issues
- Configurable fork digests for filtering IP addresses
- Dockerized for easy deployment
- Automatically sources IPs from your existing database tables
- **One-time job mode** for batch processing without continuous operation

## Requirements

- Docker and Docker Compose
- ClickHouse database (cloud or self-hosted)
- ipinfo.io API token

## Important Note About ClickHouse Connection

This application uses the `clickhouse-connect` library for ClickHouse communication, which is better suited for ClickHouse Cloud connections than the `clickhouse-driver` package.

## Quick Start

### Continuous Mode (Default)
1. Clone this repository
2. Copy `.env.example` to `.env` and update with your credentials
3. Run the service with Docker Compose

```bash
cp .env.example .env
# Edit .env with your settings
docker-compose up -d
```

### One-Time Job Mode
Run the crawler once to process a single batch of IPs and exit:

```bash
# Local execution
python -m src.crawler --once

# With custom batch size
python -m src.crawler --once --batch-size 200

# Docker execution
docker-compose run --rm -e CRAWLER_MODE=once ip-crawler
```

## One-Time Job Mode

The crawler can be run as a one-time job instead of a continuous service. This is useful for:
- Processing batches on demand
- Scheduled jobs (cron, Kubernetes CronJob)
- Testing and development
- Resource-constrained environments

### Usage

**Command Line:**
```bash
python -m src.crawler --once [--batch-size N]
```

**Docker:**
```bash
# Set environment variable
export CRAWLER_MODE=once
docker-compose up

# Or inline
docker-compose run --rm -e CRAWLER_MODE=once ip-crawler
```

**Output:**
The one-time mode provides a summary of the batch processing results:
```
==================================================
SINGLE RUN SUMMARY
==================================================
Total IPs processed: 150
Successful: 147
Failed: 3
Success rate: 98.0%
Total in database: 15,420
Overall success rate: 96.8%
==================================================
```

**Statistics File:**
Results are saved to `logs/last_run_stats.json` for programmatic access.

## Environment Variables

All configuration is handled through environment variables in the `.env` file:

### ClickHouse Connection
- `CLICKHOUSE_HOST` - ClickHouse server hostname
- `CLICKHOUSE_PORT` - ClickHouse server port
- `CLICKHOUSE_USER` - ClickHouse username
- `CLICKHOUSE_PASSWORD` - ClickHouse password
- `CLICKHOUSE_DATABASE` - Database name (default: crawlers_data)
- `CLICKHOUSE_SECURE` - Use secure connection (true/false)

### Table Configuration
- `IP_SOURCE_TABLE` - Table containing IP addresses to process (default: ip_addresses)
- `IP_INFO_TABLE` - Table to store IP information (default: ipinfo)

### IPInfo API Configuration
- `IPINFO_API_TOKEN` - ipinfo.io API token
- `IPINFO_RATE_LIMIT` - Requests per day limit (default: 1000)

### Crawler Settings
- `BATCH_SIZE` - Number of IPs to process in a batch (default: 50)
- `SLEEP_INTERVAL` - Seconds to wait between batches (default: 60)
- `REQUEST_TIMEOUT` - Seconds for API requests timeout (default: 10)
- `MAX_RETRIES` - Maximum number of retries for failed requests (default: 3)
- `RETRY_DELAY` - Seconds between retries (default: 5)
- `FORK_DIGESTS` - Comma-separated list of fork digests to track (default: 0x56fdb5e0,0x824be431,0x21a6f836,0x3ebfd484,0x7d5aab40,0xf9ab5f85)
- `CRAWLER_MODE` - Set to `once` for one-time job mode (Docker only)

## Database Schema

The application creates the main `ipinfo` table to store IP information:

```sql
CREATE TABLE ipinfo (
    ip String,
    hostname String,
    city String,
    region String,
    country String,
    loc String,
    org String,
    postal String,
    timezone String,
    asn String,
    company String,
    carrier String,
    is_bogon Boolean DEFAULT false,
    is_mobile Boolean DEFAULT false,
    abuse_email String,
    abuse_phone String,
    error String,
    attempts UInt8 DEFAULT 1,
    success Boolean DEFAULT true,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ip, updated_at);
```

## Adding IPs to Process

The crawler automatically fetches IPs from the `nebula.visits` table that haven't been processed yet. It uses queries that process the table incrementally by month to avoid memory issues. The core filtering logic looks for:

```sql
SELECT DISTINCT toString(ip) AS ip
FROM (
    SELECT JSONExtractString(toString(peer_properties), 'ip') AS ip
    FROM nebula.visits
    WHERE toStartOfMonth(visit_started_at) = toDate('YYYY-MM-01')
    AND (
        JSONExtractString(toString(peer_properties), 'fork_digest') IN ('0x56fdb5e0', '0x824be431', '0x21a6f836', '0x3ebfd484', '0x7d5aab40', '0xf9ab5f85')
        OR JSONExtractString(toString(peer_properties), 'next_fork_version') LIKE '%064%'
    )
)
WHERE ip != ''
LIMIT {batch_size}
```

## Incremental Processing

To handle very large tables without encountering memory limitations, the crawler:

1. Processes data month by month using the table's time partitioning
2. Maintains state in a JSON file to track which months have been processed
3. Automatically resumes from where it left off if restarted

## Updating Fork Digests

The list of fork digests to track can be updated in two ways:

1. By changing the `FORK_DIGESTS` environment variable in your `.env` file and restarting the container
2. By updating the environment variable while the container is running (it will detect the change automatically)

## Monitoring

The crawler creates log files in the `logs` directory, which is mounted as a volume. You can monitor the crawler's activity with:

```bash
docker-compose logs -f
```

The logs directory also contains:
- `crawler.log` - Main application logs
- `health.log` - Current status for healthcheck
- `partition_state.json` - Tracks which months have been processed
- `last_run_stats.json` - Statistics from the last one-time job run

## Health Checks

The container includes a health check that verifies the crawler is running by checking for the existence of a health log file.

## Development

### Project Structure

```
.
├── .env.example           # Template for environment variables
├── docker-compose.yml     # Docker Compose configuration
├── Dockerfile             # Docker container definition
├── entrypoint.sh          # Container entrypoint script
├── migrations/            # Database migration SQL files
│   ├── 01_create_database.sql
│   ├── 02_create_ipinfo_table.sql
├── README.md              # Project documentation
├── requirements.txt       # Python dependencies
└── src/                   # Source code
    ├── __init__.py
    ├── config.py          # Configuration management
    ├── crawler.py         # Main crawler logic
    ├── db.py              # Database interaction
    ├── migrations.py      # Database migration runner
    ├── partition_tracker.py # Manages incremental processing
    └── utils.py           # Utility functions
```

### Running Locally for Development

For development without Docker:

1. Create a virtual environment and install requirements
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your configuration
3. Run the migrations
   ```bash
   python -m src.migrations
   ```
4. Start the crawler (continuous mode)
   ```bash
   python -m src.crawler
   ```
5. Or run once
   ```bash
   python -m src.crawler --once
   ```

## Troubleshooting

### Memory Limit Exceeded

If you were previously encountering `MEMORY_LIMIT_EXCEEDED` errors when querying large tables, the incremental processing approach should solve this issue. The crawler now processes data month by month to keep memory usage low.

### Checking Processing Status

To check which months have been processed, examine the `partition_state.json` file in the logs directory. It contains information about:
- The last fully processed month
- The current month being processed
- Whether the current month is complete
- The list of fork digests being tracked

### Resetting Processing

If you need to start processing from scratch, simply stop the container and delete the `partition_state.json` file from the logs directory.

### No New IPs Found

If the one-time job reports "No new IPs to process", it means:
- All IPs in the current partition have already been processed
- The current partition is complete and no new partitions are available
- Check `partition_state.json` to see the current processing status

## License

This project is licensed under the [MIT License](LICENSE).