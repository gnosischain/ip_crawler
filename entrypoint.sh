#!/bin/bash
set -e

echo "Starting IP Info Crawler..."

# First run migrations to ensure database and tables exist
echo "Running database migrations..."
python -m src.migrations

# Check if single-run mode is requested via environment variable
if [ "$CRAWLER_MODE" = "once" ] || [ "$CRAWLER_MODE" = "single-run" ]; then
    echo "Starting crawler in single-run mode..."
    exec python -m src.crawler --once
else
    echo "Starting crawler in continuous mode..."
    exec python -m src.crawler
fi