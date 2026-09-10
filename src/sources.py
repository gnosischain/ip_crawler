"""Pure helpers for the crawler: time windows, work-list SQL and run summaries.

Nothing in this module does I/O, so tests import it freely and the SQL a run will
send can be inspected without a database.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# Settings sent with every work-list query. Constants, not env: they encode what the
# shared ClickHouse instance tolerates, not an operator preference.
WORKLIST_SETTINGS: Dict[str, Any] = {
    "max_threads": 2,                    # cap parallel read streams -> peak memory in the tens of MB
    "max_execution_time": 600,           # the server kills a stuck query instead of holding memory
    "max_memory_usage": 1_073_741_824,   # 1 GiB fuse: if anything is cut, it is us, with a clean code 241
    "log_comment": "ip_crawler worklist",  # findable in system.query_log when someone debugs pressure
}

# Server exception codes worth one retry after a pause; everything else fails the run.
RETRYABLE_CH_CODES = frozenset({241, 159, 160, 202})  # memory limit, timeout, too slow, too many queries

# Below this many attempted lookups a run with zero successes is not proof of a broken
# token or API, so it exits 0 and the next run simply retries (see exit_code_for).
MIN_ATTEMPTED_FOR_TOTAL_FAILURE = 20

NEBULA_SOURCE_SQL = """
SELECT toString(peer_properties.ip) AS ip
FROM {source_table}
WHERE visit_started_at >= toDateTime64({{since:String}}, 3, 'UTC')
  AND visit_started_at <  toDateTime64({{until:String}}, 3, 'UTC')
  AND (toString(peer_properties.fork_digest) IN ({{digests:Array(String)}})
       OR toString(peer_properties.next_fork_version) LIKE '%064%')
""".strip()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 date or datetime; naive values are taken as UTC."""
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_window(
    now: datetime,
    lookback_days: int,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> Tuple[datetime, datetime]:
    """Half-open [since, until) window in UTC.

    `until` defaults to `now`; `since` defaults to `until - lookback_days`.
    """
    until_dt = parse_iso_utc(until) if until else now.astimezone(timezone.utc)
    since_dt = parse_iso_utc(since) if since else until_dt - timedelta(days=lookback_days)
    if since_dt >= until_dt:
        raise ValueError(f"window is empty: since={since_dt.isoformat()} >= until={until_dt.isoformat()}")
    return since_dt, until_dt


def compute_sweep_window(
    recent_since: datetime, now: datetime, sweep_lookback_days: int
) -> Optional[Tuple[datetime, datetime]]:
    """The half-open window the sweep phase repairs: [now - sweep_lookback_days, recent_since).

    Anchored on the recent phase's start so the two phases are contiguous and never
    overlap. None when disabled (days <= 0) or when it would be empty.
    """
    if sweep_lookback_days <= 0:
        return None
    since = now.astimezone(timezone.utc) - timedelta(days=sweep_lookback_days)
    until = recent_since.astimezone(timezone.utc)
    if since >= until:
        return None
    return since, until


def chunk_window(since: datetime, until: datetime, chunk_hours: int) -> List[Tuple[datetime, datetime]]:
    """Split [since, until) into consecutive half-open chunks of at most `chunk_hours`.

    A chunk_hours <= 0 means a single chunk. The last chunk may be shorter.
    """
    if chunk_hours <= 0:
        return [(since, until)]
    step = timedelta(hours=chunk_hours)
    chunks: List[Tuple[datetime, datetime]] = []
    start = since
    while start < until:
        end = min(start + step, until)
        chunks.append((start, end))
        start = end
    return chunks


def ch_datetime64(dt: datetime) -> str:
    """Format for toDateTime64({x:String}, 3, 'UTC'): 'YYYY-MM-DD HH:MM:SS.mmm' in UTC."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def nebula_source_sql(source_table: str) -> str:
    return NEBULA_SOURCE_SQL.format(source_table=source_table)


def nebula_params(since: datetime, until: datetime, digests: Sequence[str]) -> Dict[str, Any]:
    return {"since": ch_datetime64(since), "until": ch_datetime64(until), "digests": list(digests)}


def wrap_work_list(inner_sql: str, database: str, table: str) -> str:
    """Turn any source SELECT that yields a column named `ip` into the work list.

    The anti-join against the ipinfo table and the cap both run server-side, so a
    source is never asked for rows the crawler would discard. The caller binds
    `max_ips` (send LIMIT max_ips + 1 to detect truncation).
    """
    inner = inner_sql.strip().rstrip(";")
    return (
        "SELECT DISTINCT ip FROM (\n"
        f"{inner}\n"
        f") WHERE ip != '' AND ip NOT IN (SELECT ip FROM {database}.{table})\n"
        "LIMIT {max_ips:UInt32}"
    )


_BANNED_KEYWORDS = (
    "insert ", "alter ", "drop ", "create ", "truncate ",
    "attach ", "detach ", "system ", "optimize ", "delete ", "rename ", "grant ",
)


def validate_readonly_select(sql: str) -> str:
    """Accept a single bare SELECT/WITH statement; return it cleaned. Raise ValueError otherwise.

    Ad-hoc sources can come from an env var or the command line, so they are checked
    rather than trusted. This is a guard against mistakes, not a security boundary.
    """
    cleaned = sql.strip().rstrip(";").strip()
    lowered = cleaned.lower()
    if not lowered.startswith(("select", "with")):
        raise ValueError(f"IP source query must be a SELECT/WITH, got: {cleaned[:60]!r}")
    for banned in _BANNED_KEYWORDS:
        if banned in lowered:
            raise ValueError(f"IP source query contains a non-read statement: {banned.strip()!r}")
    if ";" in cleaned:
        raise ValueError("IP source query must be a single statement (no ';')")
    return cleaned


def dedupe_keep_order(ips: Iterable[Any]) -> List[str]:
    seen: set = set()
    out: List[str] = []
    for raw in ips:
        if raw is None:
            continue
        ip = str(raw).strip()
        if ip and ip not in seen:
            seen.add(ip)
            out.append(ip)
    return out


SUMMARY_KEYS = (
    "event", "source", "mode", "window", "candidates", "truncated", "looked_up",
    "saved_ok", "saved_failed", "lookup_failed", "skipped_existing", "skipped_shutdown",
    "clickhouse_errors", "duration_s", "dry_run", "exit_code",
    "phases",  # appended last, 2026-09: per-phase detail of a two-phase nightly run, else None
)

# Keys summed across phases by merge_summaries.
COUNTER_KEYS = (
    "candidates", "looked_up", "saved_ok", "saved_failed", "lookup_failed",
    "skipped_existing", "skipped_shutdown", "clickhouse_errors",
)
_PHASE_VIEW_DROP = frozenset({"event", "source", "mode", "dry_run", "exit_code", "phases"})


def build_summary(**fields: Any) -> Dict[str, Any]:
    """One flat JSON-able object with a stable key order; unknown keys are rejected."""
    unknown = set(fields) - set(SUMMARY_KEYS)
    if unknown:
        raise KeyError(f"unknown summary fields: {sorted(unknown)}")
    summary: Dict[str, Any] = {"event": "run_summary"}
    for key in SUMMARY_KEYS:
        if key == "event":
            continue
        summary[key] = fields.get(key, 0 if key not in ("source", "mode", "window", "phases") else None)
    for flag in ("truncated", "dry_run"):
        summary[flag] = bool(summary[flag])
    return summary


def exit_code_for(summary: Dict[str, Any], fatal: bool = False) -> int:
    """1 on a fatal error or when a meaningful number of lookups all failed; else 0."""
    if fatal:
        return 1
    if summary.get("looked_up", 0) >= MIN_ATTEMPTED_FOR_TOTAL_FAILURE and summary.get("saved_ok", 0) == 0:
        return 1
    return 0


def phase_view(summary: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """A phase's own numbers without the run-level fields (event, mode, exit code...)."""
    if summary is None:
        return None
    return {k: v for k, v in summary.items() if k not in _PHASE_VIEW_DROP}


def exit_code_for_phases(phases: Sequence[Optional[Dict[str, Any]]], merged: Dict[str, Any]) -> int:
    """Worst of the merged rule and each phase's own rule.

    A phase whose 20+ lookups all failed is evidence of a broken API on its own; the
    merged check catches two small phases that only cross the threshold together.
    """
    codes = [exit_code_for(merged)] + [exit_code_for(p) for p in phases if p]
    return max(codes)


def merge_summaries(recent: Dict[str, Any], sweep: Optional[Dict[str, Any]], duration_s: float) -> Dict[str, Any]:
    """One run_summary for a two-phase run.

    Top-level counters are the sums; `window`, `truncated`, `source`, `mode` and `dry_run`
    describe the recent phase (so an alert on `truncated` keeps meaning "the nightly
    window itself hit the cap"); each phase's own numbers sit under `phases`.
    """
    merged = dict(recent)
    if sweep:
        for key in COUNTER_KEYS:
            merged[key] = int(recent.get(key, 0) or 0) + int(sweep.get(key, 0) or 0)
    merged["duration_s"] = round(duration_s, 1)
    merged["phases"] = {"recent": phase_view(recent), "sweep": phase_view(sweep)}
    merged["exit_code"] = exit_code_for_phases([recent, sweep], merged)
    return merged
