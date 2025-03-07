# IP Info Crawler

A robust service that continuously fetches IP address information from ipinfo.io and stores it in a ClickHouse database.

## Features

- Connects to ClickHouse Cloud
- Creates necessary database and tables if they don't exist
- Handles rate limiting for the ipinfo.io API (up to 10 requests per second)
- Implements retry logic and error handling
- Processes IPs in batches for efficiency
- Dockerized for easy deployment
- Automatically sources IPs from your existing database tables

## Requirements

- Docker and Docker Compose
- ClickHouse database (cloud or self-hosted)
- ipinfo.io API token

## Important Note About ClickHouse Connection

This application uses the `clickhouse-connect` library for ClickHouse communication, which is better suited for ClickHouse Cloud connections than the `clickhouse-driver` package.

## Quick Start

1. Clone this repository
2. Copy `.env.example` to `.env` and update with your credentials
3. Run the service with Docker Compose

```bash
cp .env.example .env
# Edit .env with your settings
docker-compose up -d
```

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

The crawler automatically fetches IPs from the `nebula.visits` table that haven't been processed yet. It uses the query:

```sql
WITH source AS (
    SELECT DISTINCT toString(ip) AS ip
    FROM (
        SELECT JSONExtractString(toString(peer_properties), 'ip') AS ip
        FROM nebula.visits
        WHERE toString(peer_properties) LIKE '%064%'
    )
    WHERE ip != ''
)

SELECT ip FROM source
WHERE ip NOT IN (
    SELECT ip FROM crawlers_data.ipinfo
)
LIMIT {batch_size}
```

## Monitoring

The crawler creates log files in the `logs` directory, which is mounted as a volume. You can monitor the crawler's activity with:

```bash
docker-compose logs -f
```

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
│   └── 03_create_ip_addresses_table.sql
├── README.md              # Project documentation
├── requirements.txt       # Python dependencies
└── src/                   # Source code
    ├── __init__.py
    ├── config.py          # Configuration management
    ├── crawler.py         # Main crawler logic
    ├── db.py              # Database interaction
    └── migrations.py      # Database migration runner
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
4. Start the crawler
   ```bash
   python -m src.crawler
   ```