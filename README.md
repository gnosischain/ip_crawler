# IP Info Crawler

Enriches peer IPs seen by the [Nebula](https://github.com/dennis-tra/nebula) network crawler with
[ipinfo.io](https://ipinfo.io) geolocation, into the ClickHouse table `crawlers_data.ipinfo`.
It is a batch job: one run finds the IPs that appeared recently and are not yet in the table,
looks them up under the API rate limit, writes them, prints one JSON summary line and exits.

## How a run works

```
source ──► work list ──► enrich ──► summary
```

1. **Source.** A read-only `SELECT` that yields a column named `ip`. The default source is the
   Nebula window: visits in `[now - LOOKBACK_DAYS, now)` whose peer advertises a Gnosis fork digest.
   Named presets (`--source hopr`) and ad-hoc queries (`--ips-query`) are sources too.
2. **Work list.** The source is wrapped server-side:
   `SELECT DISTINCT ip FROM (source) WHERE ip != '' AND ip NOT IN (SELECT ip FROM crawlers_data.ipinfo) LIMIT MAX_IPS_PER_RUN`.
   The anti-join is the only state the crawler has: an IP is looked up once, ever. The Nebula
   window is queried in `WINDOW_CHUNK_HOURS` chunks so no single query scans more than a day, and
   the query carries `max_threads=2`, `max_execution_time=600` and a 1 GiB `max_memory_usage` fuse.
3. **Enrich.** Each IP is checked once more against the table (guards against overlapping jobs),
   fetched from `https://ipinfo.io/{ip}` at most once per `86400 / (IPINFO_RATE_LIMIT * 0.95)`
   seconds, flattened and inserted.
4. **Summary.** One line on stdout and in `logs/last_run_stats.json`. Top-level counters are the
   whole run; `phases` carries each phase's own numbers (see the next section):

```json
{"event":"run_summary","source":"nebula","mode":"once","window":{"since":"2026-09-05T12:00:00+00:00","until":"2026-09-07T12:00:00+00:00","lookback_days":2.0,"chunks":2},"candidates":378,"truncated":false,"looked_up":378,"saved_ok":374,"saved_failed":1,"lookup_failed":3,"skipped_existing":0,"skipped_shutdown":0,"clickhouse_errors":0,"duration_s":760.1,"dry_run":false,"exit_code":0,"phases":{"recent":{"window":{"since":"2026-09-05T12:00:00+00:00","until":"2026-09-07T12:00:00+00:00","lookback_days":2.0,"chunks":2},"candidates":370,"truncated":false,"looked_up":370,"saved_ok":366,"saved_failed":1,"lookup_failed":3,"skipped_existing":0,"skipped_shutdown":0,"clickhouse_errors":0,"duration_s":694.2},"sweep":{"window":{"since":"2026-08-08T12:00:00+00:00","until":"2026-09-05T12:00:00+00:00","lookback_days":28.0,"chunks":28},"candidates":8,"truncated":false,"looked_up":8,"saved_ok":8,"saved_failed":0,"lookup_failed":0,"skipped_existing":0,"skipped_shutdown":0,"clickhouse_errors":0,"duration_s":65.9,"budget_exhausted":false,"max_ips":500,"max_seconds":3600}}}
```

### The sweep phase (backlog repair)

`--once` is a **two-phase** run. Phase one is the recent window above. Phase two, the sweep, walks
`[now - SWEEP_LOOKBACK_DAYS, now - LOOKBACK_DAYS)` with the same chunked anti-join query,
**oldest chunk first**, under its own small cap (`SWEEP_MAX_IPS_PER_RUN`, default 500 ≈ 15 min of
lookups) and wall-clock budget (`SWEEP_MAX_SECONDS`, default 1 h). The two windows are contiguous
and never overlap.

Why it exists: the recent window keeps up with new peers but cannot *recover*. A peer whose visits
all predate the window and that was never enriched (an outage, a night at the cap) would otherwise
stay invisible. With the sweep, such a backlog drains by itself over the following nights, oldest
days first, and because the anti-join excludes everything already enriched, a clean history costs
only ~30 two-second queries and zero API calls per night. One CronJob does both, so nothing can
overlap and the monthly budget is bounded by construction: at most
`MAX_IPS_PER_RUN + SWEEP_MAX_IPS_PER_RUN` lookups per night.

Reading the summary: top-level `truncated` and `window` describe the recent phase only, so an alert
on `truncated` still means "the nightly window itself hit the cap". `phases.sweep.truncated` or
`phases.sweep.budget_exhausted` mean "still draining, more tomorrow". A sweep whose work-list query
fails is recorded under `phases.sweep.error` with `clickhouse_errors = 1` and does **not** fail the
run: the recent phase already did the night's job. `SWEEP_LOOKBACK_DAYS=0` disables the phase;
explicit `--since/--until` runs are always single-phase.

Measured on production on 2026-09-07: a two-day window is about 370 candidates, 2.5 s of
ClickHouse time and ~11 minutes of rate-limited API calls.

### Failure semantics

| What happened | Effect |
|---|---|
| ipinfo `400`/`404`, or an undecodable body | a row with `success = false` is written; the IP is never retried |
| timeout, connection error, `5xx`, `429` after retries | counted in `lookup_failed`, **not** written; the next run's anti-join picks it up |
| ipinfo `401`/`403` | fatal: `run_failure` with `reason: auth`, exit 1 |
| work-list query fails (after one retry on memory-limit/timeout codes) | fatal: `reason: worklist`, exit 1 |
| `looked_up >= 20` and `saved_ok == 0` | exit 1: a broken run must not look green |
| bad `--since/--until`, unknown source | exit 2 |
| `SIGTERM` (e.g. the Job deadline) | current IP finishes, the rest is `skipped_shutdown`, summary still printed, exit 0 |

Everything else, including a truncated run, exits 0. Re-running is always safe.

## Running

```bash
# The nightly job (what the Kubernetes CronJob does; CRAWLER_MODE=once in a container):
# recent window, then the sweep phase
python -m src.crawler --once

# Explicit window, e.g. a backfill of August 2026, or a wider sweep
python -m src.crawler --since 2026-08-01 --until 2026-09-01
python -m src.crawler --once --lookback-days 30

# Dry run: fetch and print the work list, no API calls, no inserts (also DRY_RUN=true / CRAWLER_MODE=dry-run)
python -m src.crawler --once --dry-run

# Explicit sources (IP_SOURCE=hopr in a container)
python -m src.crawler --source hopr
python -m src.crawler --ips-query "SELECT DISTINCT some_column AS ip FROM some.table"

# Continuous: the same window run every SLEEP_INTERVAL seconds (anything else as CRAWLER_MODE)
python -m src.crawler
```

With Docker Compose: `docker compose run --rm ip-crawler-once`, `ip-crawler-dry-run`,
`ip-crawler-source`, or `docker compose up -d ip-crawler` for the loop. Copy `.env.example` to
`.env` first.

### Explicit IP sources

Presets live in `IP_SOURCE_QUERIES` in `src/config.py`. A preset returns one column named `ip`;
the work-list wrapper handles de-duplication, the anti-join and the cap.

| Preset | IPs | Source table |
|---|---|---|
| `hopr` | IPv4 addresses HOPR mixnet nodes announced on-chain | `HOPR_NODES_TABLE` (default `dbt.int_hopr_nodes`) |

`hopr` reads a dbt model and writes a table dbt reads back, so it wants to run after dbt, and the
crawler's ClickHouse user needs `SELECT` on the `dbt` database; without it the run exits 1 with an
`ACCESS_DENIED` in the `run_failure` line. Ad-hoc queries must be a single bare `SELECT`/`WITH`;
anything containing a write keyword or a second statement is refused.

## Configuration

All values come from the environment (a local `.env` is loaded if present).

| Variable | Default | Meaning |
|---|---|---|
| `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` | `localhost` / `8443` | HTTP(S) port; clickhouse-connect does not speak the native protocol |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / empty | |
| `CLICKHOUSE_DATABASE` / `IP_INFO_TABLE` | `crawlers_data` / `ipinfo` | target table; migrations use the same names |
| `CLICKHOUSE_SECURE` | `true` | TLS |
| `NEBULA_VISITS_TABLE` | `nebula.visits` | source of the default window job |
| `IPINFO_API_TOKEN` | empty | required for lookups |
| `IPINFO_RATE_LIMIT` | `50000` | requests per day; `0` disables throttling |
| `REQUEST_TIMEOUT` / `MAX_RETRIES` / `RETRY_DELAY` | `10` / `3` / `5` | per-IP HTTP behaviour |
| `LOOKBACK_DAYS` | `2` | window length for `--once` and continuous mode |
| `WINDOW_CHUNK_HOURS` | `24` | one ClickHouse query per chunk |
| `MAX_IPS_PER_RUN` | `10000` | cap on lookups in the recent phase (about 5 h at the default rate) |
| `SWEEP_LOOKBACK_DAYS` | `30` | how far back the sweep phase repairs; `0` disables it |
| `SWEEP_MAX_IPS_PER_RUN` | `500` | cap on lookups in the sweep phase per run |
| `SWEEP_MAX_SECONDS` | `3600` | wall-clock budget for the sweep phase; `0` = unbounded |
| `FORK_DIGESTS` | Gnosis digests | comma-separated; peers advertising one of these are Gnosis peers |
| `DRY_RUN` | `false` | `true`: print the work list only |
| `CRAWLER_MODE` | continuous | `once` / `single-run` / `dry-run` / anything else (entrypoint only) |
| `IP_SOURCE` | unset | run a named preset instead of the window job (entrypoint only) |
| `BATCH_SIZE` | `50` | progress-log cadence |
| `SLEEP_INTERVAL` | `3600` | pause between runs in continuous mode |
| `HOPR_NODES_TABLE` | `dbt.int_hopr_nodes` | source table of the `hopr` preset |

## Schema

`src/migrations.py` runs `migrations/*.sql` on every start; the files are idempotent
(`IF NOT EXISTS`) and templated on `{{DATABASE}}` / `{{TABLE}}`.

```sql
CREATE TABLE IF NOT EXISTS crawlers_data.ipinfo (
    ip String, hostname String, city String, region String, country String, loc String,
    org String, postal String, timezone String, asn String, company String, carrier String,
    is_bogon Boolean DEFAULT false, is_mobile Boolean DEFAULT false,
    abuse_email String, abuse_phone String,
    error String, attempts UInt8 DEFAULT 1, success Boolean DEFAULT true,
    created_at DateTime DEFAULT now(), updated_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY (ip, updated_at);
```

The table is a plain `MergeTree`: an IP that was written twice (two overlapping jobs, or a retry
whose first insert succeeded) has two rows. Readers that need one row per IP should use
`LIMIT 1 BY ip`.

## Operations notes

- **Scheduling.** One Kubernetes CronJob with `concurrencyPolicy: Forbid` and an
  `activeDeadlineSeconds` around 8–10 h is enough; the sweep phase replaces any separate weekly
  job. Worst case per night is the recent cap plus the sweep cap plus the sweep's time budget. A
  run that stops at the deadline or a cap leaves the rest for the next run; nothing is lost.
- **Health.** `logs/health.log` is rewritten at start, every `BATCH_SIZE` IPs and at the end; the
  container probe only checks that it exists.
- **Alerting.** Key on the summary line: absence of `event="run_summary"` for 26 h, `exit_code != 0`,
  `truncated=true` (the recent window hit its cap), or `clickhouse_errors > 0` (the sweep's query
  is failing night after night). `phases.sweep.truncated` on its own is normal while a backlog drains.
- **Budget.** `MAX_IPS_PER_RUN` is the throttle if the ipinfo plan is smaller than the daily limit
  suggests; check the plan before enabling a wide sweep.

## Development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest
```

Tests mock ClickHouse and HTTP; nothing in the suite touches the network. CI runs them before
every image build; images are pushed to `ghcr.io/gnosischain/gc-ip_crawler:<short sha>` and
`:latest` on every push to `main`.

Layout: `src/sources.py` (pure: windows, SQL, summaries), `src/db.py` (ClickHouse), `src/crawler.py`
(lookups, runs, CLI), `src/utils.py` (response flattening), `src/migrations.py`, `src/config.py`.
