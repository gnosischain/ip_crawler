#!/bin/bash
set -e

echo "Starting IP Info Crawler..."

# First run migrations to ensure database and tables exist
echo "Running database migrations..."
python -m src.migrations

# Start the crawler
echo "Starting crawler..."
exec python -m src.crawler