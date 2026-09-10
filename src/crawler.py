"""IP Info Crawler: source -> work list -> enrich -> summary.

A run asks ClickHouse for the IPs in a recent window of nebula.visits (or any other
source) that are not yet in the ipinfo table, looks each one up at ipinfo.io under
the rate limit, stores the result, and prints one JSON summary line. Nothing is
stateful between runs: the anti-join in the work-list query is the checkpoint.
"""
import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence

import requests
from requests.exceptions import RequestException

from src.config import (
    BATCH_SIZE, DRY_RUN, FORK_DIGESTS, IP_SOURCE_QUERIES, IPINFO_API_TOKEN, LOG_PATH,
    LOOKBACK_DAYS, MAX_IPS_PER_RUN, MAX_RETRIES, NEBULA_VISITS_TABLE, RATE_LIMIT_SECONDS,
    REQUEST_TIMEOUT, RETRY_DELAY, SLEEP_INTERVAL, SWEEP_LOOKBACK_DAYS, SWEEP_MAX_IPS_PER_RUN,
    SWEEP_MAX_SECONDS, WINDOW_CHUNK_HOURS,
)
from src.db import Database, WorkListError
from src.sources import (
    build_summary, chunk_window, compute_sweep_window, compute_window, exit_code_for,
    merge_summaries, nebula_params, nebula_source_sql, utcnow, validate_readonly_select,
)
from src.utils import sanitize_ip_info

logger = logging.getLogger("ip_crawler")

IPINFO_URL = "https://ipinfo.io/{ip}"


class AuthError(Exception):
    """ipinfo rejected the token (401/403): every lookup would fail, so the run must."""


class TransientLookupError(Exception):
    """Timeouts, connection errors, 5xx, 429: retried now, and by the next run if still failing."""


class PermanentLookupError(Exception):
    """Definitive 4xx or an undecodable body: recorded as a failure row, never retried."""


class RateLimiter:
    """Minimum spacing between calls, measured on a monotonic clock."""

    def __init__(self, min_interval: float, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.min_interval = max(0.0, float(min_interval))
        self._sleep = sleep
        self._clock = clock
        self._next_allowed: Optional[float] = None

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        now = self._clock()
        if self._next_allowed is not None and now < self._next_allowed:
            self._sleep(self._next_allowed - now)
            now = self._next_allowed
        self._next_allowed = now + self.min_interval


def write_health(note: str) -> None:
    """The liveness probe only checks this file exists; the content is for humans."""
    try:
        with open(os.path.join(LOG_PATH, "health.log"), "w") as fh:
            fh.write(f"{note} at {datetime.now().isoformat()}\n")
    except OSError as exc:
        logger.warning(f"Could not write health file: {exc}")


def emit_summary(summary: Dict[str, Any]) -> None:
    """One JSON line on stdout (for log-based alerting) and a copy on disk."""
    line = json.dumps(summary, separators=(",", ":"), default=str)
    print(line, flush=True)
    try:
        with open(os.path.join(LOG_PATH, "last_run_stats.json"), "w") as fh:
            fh.write(line + "\n")
    except OSError as exc:
        logger.warning(f"Could not write last_run_stats.json: {exc}")


def emit_failure(reason: str, error: str, exit_code: int = 1) -> None:
    emit_summary({"event": "run_failure", "reason": reason, "error": error[:500], "exit_code": exit_code})


class IPInfoCrawler:
    def __init__(self, db: Optional[Database] = None, fork_digests: Optional[Sequence[str]] = None,
                 rate_limiter: Optional[RateLimiter] = None, dry_run: bool = DRY_RUN,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.db = db if db is not None else Database()
        self.fork_digests = list(fork_digests) if fork_digests else list(FORK_DIGESTS)
        self.rate_limiter = rate_limiter or RateLimiter(RATE_LIMIT_SECONDS)
        self.dry_run = bool(dry_run)
        self._sleep = sleep
        self.running = True
        self.last_stop_reason: Optional[str] = None  # "signal" | "deadline" | None, set by run_work_list
        self._install_signal_handlers()
        logger.info(
            f"Crawler ready: fork_digests={self.fork_digests} dry_run={self.dry_run} "
            f"rate_limit={self.rate_limiter.min_interval:.2f}s/request"
        )

    # -- lifecycle ------------------------------------------------------------------
    def _install_signal_handlers(self) -> None:
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, self._handle_shutdown)
        except ValueError:  # not in the main thread (tests, embedding)
            pass

    def _handle_shutdown(self, signum, frame) -> None:  # noqa: ARG002
        logger.info(f"Received signal {signum}; finishing the current IP and stopping")
        self.running = False

    # -- one lookup -----------------------------------------------------------------
    def _http_get(self, ip: str) -> requests.Response:
        self.rate_limiter.wait()
        return requests.get(
            IPINFO_URL.format(ip=ip),
            headers={"Authorization": f"Bearer {IPINFO_API_TOKEN}", "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )

    def fetch_ip_info(self, ip: str) -> Dict[str, Any]:
        """Look one IP up, retrying transient failures up to MAX_RETRIES attempts."""
        attempts = max(1, MAX_RETRIES)
        last_error: Optional[TransientLookupError] = None
        for attempt in range(1, attempts + 1):
            try:
                response = self._http_get(ip)
            except RequestException as exc:
                last_error = TransientLookupError(f"{type(exc).__name__}: {exc}")
            else:
                status = response.status_code
                if status == 200:
                    try:
                        return response.json()
                    except ValueError as exc:
                        raise PermanentLookupError(f"undecodable response body: {exc}") from exc
                if status in (401, 403):
                    raise AuthError(f"ipinfo returned {status}: check IPINFO_API_TOKEN")
                if status == 429:
                    retry_after = _int_or(response.headers.get("Retry-After"), RETRY_DELAY)
                    logger.warning(f"ipinfo rate limit hit; waiting {retry_after}s")
                    self._sleep(retry_after)
                    last_error = TransientLookupError("rate limited (429)")
                elif 400 <= status < 500:
                    raise PermanentLookupError(f"API error: {status}")
                else:
                    last_error = TransientLookupError(f"API error: {status}")
            if attempt < attempts:
                self._sleep(min(RETRY_DELAY * attempt, 30))
        assert last_error is not None
        raise last_error

    def process_ip(self, ip: str) -> str:
        """Returns 'saved' | 'saved_failed' | 'lookup_failed' | 'skipped_existing'."""
        if self.db.ip_exists(ip):
            return "skipped_existing"
        try:
            payload = self.fetch_ip_info(ip)
        except PermanentLookupError as exc:
            logger.warning(f"{ip}: permanent failure, recording it: {exc}")
            self.db.save_ip_info({"ip": ip}, success=False, error=str(exc))
            return "saved_failed"
        except TransientLookupError as exc:
            logger.warning(f"{ip}: transient failure, left for the next run: {exc}")
            return "lookup_failed"
        info = sanitize_ip_info(payload)
        if not info.get("ip"):
            info["ip"] = ip
        self.db.save_ip_info(info)
        return "saved"

    # -- runs -----------------------------------------------------------------------
    def run_work_list(self, ips: Sequence[str], source: str, mode: str,
                      window: Optional[Dict[str, Any]] = None, truncated: bool = False,
                      started_at: Optional[float] = None, clickhouse_errors: int = 0,
                      deadline: Optional[float] = None, worklist_file: str = "worklist.txt",
                      log_tag: Optional[str] = None) -> Dict[str, Any]:
        """Enrich a finished work list and return the run summary.

        `deadline` is a time.monotonic() value; past it the remaining IPs are left for the
        next run (counted as skipped_shutdown, last_stop_reason = "deadline").
        """
        started = started_at if started_at is not None else time.monotonic()
        self.last_stop_reason = None
        tag = log_tag or source
        ips = list(ips)
        counts = {"saved_ok": 0, "saved_failed": 0, "lookup_failed": 0,
                  "skipped_existing": 0, "skipped_shutdown": 0}
        outcome_key = {"saved": "saved_ok", "saved_failed": "saved_failed",
                       "lookup_failed": "lookup_failed", "skipped_existing": "skipped_existing"}
        looked_up = 0
        write_health(f"{mode} run started, {len(ips)} candidates")
        logger.info(f"[{tag}] {len(ips)} IP(s) to look up" + (" (truncated)" if truncated else ""))

        if self.dry_run:
            path = os.path.join(LOG_PATH, worklist_file)
            with open(path, "w") as fh:
                fh.write("\n".join(ips) + ("\n" if ips else ""))
            preview = ", ".join(ips[:50])
            logger.info(f"[{tag}] dry run: full list in {path}; first {min(50, len(ips))}: {preview}")
            mode = "dry_run"
        else:
            for index, ip in enumerate(ips, start=1):
                out_of_time = deadline is not None and time.monotonic() >= deadline
                if not self.running or out_of_time:
                    self.last_stop_reason = "signal" if not self.running else "deadline"
                    counts["skipped_shutdown"] = len(ips) - (index - 1)
                    logger.info(f"[{tag}] stopping early ({self.last_stop_reason}), "
                                f"{counts['skipped_shutdown']} IP(s) left for the next run")
                    break
                outcome = self.process_ip(ip)
                counts[outcome_key[outcome]] += 1
                if outcome != "skipped_existing":
                    looked_up += 1
                if index % max(1, BATCH_SIZE) == 0:
                    logger.info(f"[{tag}] {index}/{len(ips)} processed: {counts}")
                    write_health(f"{mode} run in progress, {index}/{len(ips)}")

        summary = build_summary(
            source=source, mode=mode, window=window, candidates=len(ips), truncated=truncated,
            looked_up=looked_up, clickhouse_errors=clickhouse_errors,
            duration_s=round(time.monotonic() - started, 1), dry_run=self.dry_run, exit_code=0,
            **counts,
        )
        summary["exit_code"] = exit_code_for(summary)
        write_health(f"{mode} run finished")
        return summary

    def run_nebula_window(self, since: datetime, until: datetime, mode: str = "once", *,
                          phase: str = "recent", max_ips: Optional[int] = None,
                          deadline: Optional[float] = None) -> Dict[str, Any]:
        """New IPs seen in nebula.visits within [since, until), oldest chunk first.

        `max_ips` defaults to MAX_IPS_PER_RUN; `deadline` (time.monotonic()) bounds both
        the chunk queries and the lookups. `phase` only labels logs and the dry-run file.
        """
        started = time.monotonic()
        cap = max_ips if max_ips is not None else MAX_IPS_PER_RUN
        tag = "nebula" if phase == "recent" else f"nebula/{phase}"
        chunks = chunk_window(since, until, WINDOW_CHUNK_HOURS)
        inner_sql = nebula_source_sql(NEBULA_VISITS_TABLE)
        ips: List[str] = []
        seen = set()
        truncated = False
        logger.info(f"[{tag}] window {since.isoformat()} -> {until.isoformat()} in {len(chunks)} chunk(s), cap {cap}")
        for chunk_since, chunk_until in chunks:
            remaining = cap - len(ips)
            if remaining <= 0:
                truncated = True
                break
            if deadline is not None and time.monotonic() >= deadline:
                logger.warning(f"[{tag}] time budget spent before chunk {chunk_since:%Y-%m-%d %H:%M}; the rest waits for the next run")
                truncated = True
                break
            chunk_ips, chunk_truncated = self.db.fetch_work_list(
                inner_sql, nebula_params(chunk_since, chunk_until, self.fork_digests), remaining
            )
            new = 0
            for ip in chunk_ips:
                if ip not in seen:
                    seen.add(ip)
                    ips.append(ip)
                    new += 1
            truncated = truncated or chunk_truncated
            logger.info(f"[{tag}] chunk {chunk_since:%Y-%m-%d %H:%M} -> {chunk_until:%Y-%m-%d %H:%M}: "
                        f"{len(chunk_ips)} new IP(s), {new} not seen in earlier chunks")
        if len(ips) > cap:
            ips = ips[:cap]
            truncated = True
        if truncated:
            logger.warning(f"[{tag}] cap {cap} or budget reached; the rest is picked up next run")
        window = {
            "since": since.isoformat(), "until": until.isoformat(),
            "lookback_days": round((until - since).total_seconds() / 86400, 3), "chunks": len(chunks),
        }
        worklist_file = "worklist.txt" if phase == "recent" else f"worklist_{phase}.txt"
        return self.run_work_list(ips, source="nebula", mode=mode, window=window, truncated=truncated,
                                  started_at=started, deadline=deadline, worklist_file=worklist_file,
                                  log_tag=tag)

    def run_nightly(self, now: Optional[datetime] = None, mode: str = "once") -> Dict[str, Any]:
        """The scheduled job: the recent window at full cap, then the sweep phase.

        The sweep repairs [now - SWEEP_LOOKBACK_DAYS, recent_since) oldest-first under
        SWEEP_MAX_IPS_PER_RUN and SWEEP_MAX_SECONDS, so anything the recent window missed
        drains over the following nights without a second CronJob. A sweep whose work-list
        query fails is recorded, not fatal: the recent phase already did tonight's job.
        """
        now = now or utcnow()
        started = time.monotonic()
        recent_since, recent_until = compute_window(now, LOOKBACK_DAYS)
        recent = self.run_nebula_window(recent_since, recent_until, mode=mode, phase="recent")

        sweep: Optional[Dict[str, Any]] = None
        sweep_window = compute_sweep_window(recent_since, now, SWEEP_LOOKBACK_DAYS)
        if sweep_window is None:
            logger.info(f"[nebula/sweep] disabled (SWEEP_LOOKBACK_DAYS={SWEEP_LOOKBACK_DAYS})")
        elif not self.running:
            logger.info("[nebula/sweep] skipped: shutdown requested during the recent phase")
        else:
            sweep_since, sweep_until = sweep_window
            deadline = time.monotonic() + SWEEP_MAX_SECONDS if SWEEP_MAX_SECONDS > 0 else None
            try:
                sweep = self.run_nebula_window(sweep_since, sweep_until, mode=mode, phase="sweep",
                                               max_ips=SWEEP_MAX_IPS_PER_RUN, deadline=deadline)
                sweep["budget_exhausted"] = self.last_stop_reason == "deadline"
                sweep["max_ips"] = SWEEP_MAX_IPS_PER_RUN
                sweep["max_seconds"] = SWEEP_MAX_SECONDS
            except WorkListError as exc:
                logger.error(f"[nebula/sweep] work list failed; the nightly run continues: {exc}")
                sweep = {
                    "window": {"since": sweep_since.isoformat(), "until": sweep_until.isoformat()},
                    "error": str(exc)[:300], "clickhouse_errors": 1,
                }
        return merge_summaries(recent, sweep, time.monotonic() - started)

    def run_source(self, name: Optional[str] = None, sql: Optional[str] = None) -> Dict[str, Any]:
        """Explicit source: a named preset from config, or an ad-hoc read-only SELECT."""
        started = time.monotonic()
        if name:
            if name not in IP_SOURCE_QUERIES:
                raise KeyError(f"unknown IP source {name!r}; known: {sorted(IP_SOURCE_QUERIES)}")
            inner_sql, label = validate_readonly_select(IP_SOURCE_QUERIES[name]), name
        elif sql:
            inner_sql, label = validate_readonly_select(sql), "ad-hoc"
        else:
            raise ValueError("run_source needs a preset name or a query")
        ips, truncated = self.db.fetch_work_list(inner_sql, {}, MAX_IPS_PER_RUN)
        return self.run_work_list(ips, source=label, mode="once", truncated=truncated, started_at=started)

    def run_continuous(self) -> None:
        """Thin loop: the same window job, then sleep SLEEP_INTERVAL, until stopped."""
        logger.info(f"Continuous mode: window job every {SLEEP_INTERVAL}s")
        while self.running:
            try:
                emit_summary(self.run_nightly(mode="continuous"))
            except AuthError:
                raise
            except Exception as exc:  # keep the loop alive; the next iteration retries
                logger.exception(f"Window job failed: {exc}")
                emit_failure("window_job", str(exc), exit_code=0)
            deadline = time.monotonic() + SLEEP_INTERVAL
            while self.running and time.monotonic() < deadline:
                self._sleep(min(5.0, max(0.0, deadline - time.monotonic())))


def _int_or(value: Optional[str], default: int) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def configure_logging() -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(os.path.join(LOG_PATH, "crawler.log")))
    except OSError:
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        handlers=handlers, force=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="IP Info Crawler")
    parser.add_argument("--once", "--single-run", action="store_true",
                        help="Run the nightly job once and exit: recent window, then the sweep phase (default in the CronJob)")
    parser.add_argument("--since", help="Window start, ISO date/datetime (UTC if naive); implies --once")
    parser.add_argument("--until", help="Window end, ISO date/datetime (default: now); implies --once")
    parser.add_argument("--lookback-days", type=int, help=f"Window length when --since is absent (default {LOOKBACK_DAYS})")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print the work list; no ipinfo calls, no inserts")
    parser.add_argument("--source", choices=sorted(IP_SOURCE_QUERIES),
                        help="Enrich a named explicit IP source instead of the nebula window")
    parser.add_argument("--ips-query", help="Enrich IPs from an ad-hoc read-only SELECT with a column named ip")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.source and args.ips_query:
        build_parser().error("--source and --ips-query are mutually exclusive")
    configure_logging()
    write_health("crawler starting")
    dry_run = bool(args.dry_run or DRY_RUN)

    try:
        crawler = IPInfoCrawler(dry_run=dry_run)
    except Exception as exc:
        logger.critical(f"Cannot connect to ClickHouse: {exc}")
        emit_failure("connect", str(exc))
        return 1

    try:
        if args.source or args.ips_query:
            summary = crawler.run_source(name=args.source, sql=args.ips_query)
        elif args.since or args.until or args.lookback_days:
            # An explicit window is a single-phase run (backfills, checks); no sweep.
            try:
                since, until = compute_window(utcnow(), args.lookback_days or LOOKBACK_DAYS, args.since, args.until)
            except ValueError as exc:
                logger.critical(f"Bad window: {exc}")
                emit_failure("window", str(exc), exit_code=2)
                return 2
            summary = crawler.run_nebula_window(since, until)
        elif args.once:
            summary = crawler.run_nightly()
        else:
            crawler.run_continuous()
            return 0
    except WorkListError as exc:
        logger.critical(f"Work list failed: {exc}")
        emit_failure("worklist", str(exc))
        return 1
    except AuthError as exc:
        logger.critical(str(exc))
        emit_failure("auth", str(exc))
        return 1
    except (KeyError, ValueError) as exc:
        logger.critical(f"Bad source: {exc}")
        emit_failure("source", str(exc), exit_code=2)
        return 2

    emit_summary(summary)
    return int(summary["exit_code"])


if __name__ == "__main__":
    sys.exit(main())
