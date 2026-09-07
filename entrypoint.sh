#!/bin/bash
set -e

echo "Starting IP Info Crawler..."

# Migrations only create the database/table if missing. A dry run must not need
# write grants, so it skips them.
if [ "$(echo "${DRY_RUN:-false}" | tr '[:upper:]' '[:lower:]')" = "true" ]; then
    echo "DRY_RUN=true: skipping migrations"
else
    echo "Running database migrations..."
    python -m src.migrations
fi

# Explicit IP source (e.g. IP_SOURCE=hopr): enrich a finite, named list and exit.
# Checked first and keyed on its own variable: CRAWLER_MODE=once is the nebula
# window job, and running that for a HOPR schedule would enrich no HOPR IPs.
if [ -n "$IP_SOURCE" ]; then
    echo "Starting crawler for explicit IP source: $IP_SOURCE"
    exec python -m src.crawler --source "$IP_SOURCE" "$@"
fi

case "${CRAWLER_MODE:-}" in
    once|single-run)
        echo "Starting crawler: one nebula window run..."
        exec python -m src.crawler --once "$@"
        ;;
    dry-run)
        echo "Starting crawler: dry run of one nebula window..."
        exec python -m src.crawler --once --dry-run "$@"
        ;;
    *)
        echo "Starting crawler in continuous mode..."
        exec python -m src.crawler "$@"
        ;;
esac
