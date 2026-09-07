"""ClickHouse access: the work-list query, the existence check and the insert."""
import logging
import re
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import (
    CLICKHOUSE_DATABASE, CLICKHOUSE_HOST, CLICKHOUSE_PASSWORD, CLICKHOUSE_PORT,
    CLICKHOUSE_SECURE, CLICKHOUSE_USER, IP_INFO_TABLE,
)
from src.sources import RETRYABLE_CH_CODES, WORKLIST_SETTINGS, dedupe_keep_order, wrap_work_list

logger = logging.getLogger("db")

_CODE_RE = re.compile(r"Code:\s*(\d+)")

# Columns written by save_ip_info, in insert order. attempts/created_at/updated_at
# keep their table defaults.
IPINFO_COLUMNS: Tuple[str, ...] = (
    "ip", "hostname", "city", "region", "country", "loc", "org", "postal", "timezone",
    "asn", "company", "carrier", "is_bogon", "is_mobile", "abuse_email", "abuse_phone",
    "error", "success",
)
_BOOL_COLUMNS = {"is_bogon", "is_mobile"}


class WorkListError(Exception):
    """The work list could not be fetched; the run cannot continue."""


def exception_code(exc: BaseException) -> Optional[int]:
    """The ClickHouse server error code embedded in a driver exception message, if any."""
    match = _CODE_RE.search(str(exc))
    return int(match.group(1)) if match else None


_transport_retry = retry(
    retry=retry_if_exception_type(ClickHouseError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)


class Database:
    def __init__(self) -> None:
        self.database = CLICKHOUSE_DATABASE
        self.table_name = IP_INFO_TABLE
        self.table = f"{CLICKHOUSE_DATABASE}.{IP_INFO_TABLE}"
        self.client: Client = self._create_client()

    # -- connection -----------------------------------------------------------------
    def _create_client(self) -> Client:
        logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            secure=CLICKHOUSE_SECURE,
        )
        client.command("SELECT 1")
        logger.info("ClickHouse connection established")
        return client

    def _reconnect(self) -> None:
        self.client = self._create_client()

    @_transport_retry
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None,
                settings: Optional[Dict[str, Any]] = None) -> List[Tuple]:
        """Run a small query with a few retries; reconnects between attempts."""
        try:
            return self.client.query(query, parameters=params, settings=settings).result_rows
        except ClickHouseError as exc:
            logger.error(f"ClickHouse query failed: {exc}")
            try:
                self._reconnect()
            except Exception as reconnect_exc:  # keep the original error as the cause
                logger.error(f"Reconnect failed: {reconnect_exc}")
            raise

    # -- work list ------------------------------------------------------------------
    def fetch_work_list(self, inner_sql: str, params: Dict[str, Any], max_ips: int,
                        retry_sleep: float = 60.0) -> Tuple[List[str], bool]:
        """IPs from `inner_sql` that are not yet in the ipinfo table, at most `max_ips`.

        Returns (ips, truncated). Server errors that mean "the instance is busy"
        (memory limit, timeout, too slow, too many queries) get one retry after a
        pause; anything else raises WorkListError immediately. Deliberately not
        behind the generic transport retry: a fatal query must fail fast.
        """
        sql = wrap_work_list(inner_sql, self.database, self.table_name)
        bound = dict(params)
        bound["max_ips"] = int(max_ips) + 1  # one extra row tells us the cap was hit
        attempt = 0
        while True:
            attempt += 1
            try:
                rows = self.client.query(sql, parameters=bound, settings=WORKLIST_SETTINGS).result_rows
                break
            except ClickHouseError as exc:
                code = exception_code(exc)
                if attempt == 1 and code in RETRYABLE_CH_CODES:
                    logger.warning(f"Work-list query hit ClickHouse code {code}; retrying once in {retry_sleep:.0f}s")
                    time.sleep(retry_sleep)
                    try:
                        self._reconnect()
                    except Exception as reconnect_exc:
                        raise WorkListError(f"reconnect after code {code} failed: {reconnect_exc}") from reconnect_exc
                    continue
                raise WorkListError(f"work-list query failed (code {code}): {exc}") from exc
        ips = dedupe_keep_order(row[0] for row in rows if row)
        truncated = len(ips) > max_ips
        return ips[:max_ips], truncated

    # -- per-IP operations ----------------------------------------------------------
    def ip_exists(self, ip: str) -> bool:
        """Point lookup on the primary key; the guard against inserting an IP twice."""
        rows = self.execute(
            f"SELECT 1 FROM {self.table} WHERE ip = {{ip:String}} LIMIT 1",
            {"ip": ip},
        )
        return len(rows) > 0

    @_transport_retry
    def _insert(self, rows: Sequence[Sequence[Any]], columns: Sequence[str]) -> None:
        try:
            self.client.insert(self.table, list(rows), column_names=list(columns))
        except ClickHouseError as exc:
            logger.error(f"ClickHouse insert failed: {exc}")
            try:
                self._reconnect()
            except Exception as reconnect_exc:
                logger.error(f"Reconnect failed: {reconnect_exc}")
            raise

    def save_ip_info(self, info: Dict[str, Any], success: bool = True, error: str = "") -> None:
        """Insert one row. `info` is the flat dict from utils.sanitize_ip_info

        (or just {'ip': ...} for a failure row); keys map to columns one to one.
        """
        row: List[Any] = []
        for column in IPINFO_COLUMNS:
            if column == "success":
                row.append(bool(success))
            elif column == "error":
                row.append(str(error or ""))
            elif column in _BOOL_COLUMNS:
                row.append(bool(info.get(column, False)))
            else:
                value = info.get(column, "")
                row.append("" if value is None else str(value))
        self._insert([row], IPINFO_COLUMNS)
        logger.info(f"Saved {'info' if success else 'failure'} for IP {info.get('ip', '')}")
