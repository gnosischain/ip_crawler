#!/bin/bash
set -e

echo "Starting IP Info Crawler..."

# First run migrations to ensure database and tables exist
echo "Running database migrations..."
python -m src.migrations

# Explicit IP source (e.g. IP_SOURCE=hopr): enrich a finite, named list and exit.
#
# Checked FIRST and deliberately keyed on its own variable rather than another
# CRAWLER_MODE value. CRAWLER_MODE=once means "one batch of the nebula crawl", which
# reads like the right setting for a daily HOPR job and is not -- it would enrich no
# HOPR IPs and consume nebula crawl budget instead, without erroring. Separate
# variables make that mistake impossible to make silently.
#
# IP_SOURCE is unset in the nebula deployment, so that container falls straight
# through to the two branches below and behaves exactly as before.
if [ -n "$IP_SOURCE" ]; then
    echo "Starting crawler for explicit IP source: $IP_SOURCE"
    exec python -m src.crawler --source "$IP_SOURCE"
fi

# Check if single-run mode is requested via environment variable
if [ "$CRAWLER_MODE" = "once" ] || [ "$CRAWLER_MODE" = "single-run" ]; then
    echo "Starting crawler in single-run mode..."
    exec python -m src.crawler --once
else
    echo "Starting crawler in continuous mode..."
    exec python -m src.crawler
fi