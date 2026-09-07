from datetime import datetime, timezone

import pytest

from src.sources import (
    MIN_ATTEMPTED_FOR_TOTAL_FAILURE,
    RETRYABLE_CH_CODES,
    WORKLIST_SETTINGS,
    build_summary,
    ch_datetime64,
    dedupe_keep_order,
    exit_code_for,
    nebula_params,
    nebula_source_sql,
    validate_readonly_select,
    wrap_work_list,
)

DIGESTS = ["0x56fdb5e0", "0x824be431"]
SINCE = datetime(2026, 9, 5, 12, 0, 0, 123_000, tzinfo=timezone.utc)
UNTIL = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


def test_nebula_sql_reads_json_subcolumns_with_utc_bounds():
    sql = nebula_source_sql("nebula.visits")
    assert "toString(peer_properties.ip) AS ip" in sql
    assert "FROM nebula.visits" in sql
    assert "visit_started_at >= toDateTime64({since:String}, 3, 'UTC')" in sql
    assert "visit_started_at <  toDateTime64({until:String}, 3, 'UTC')" in sql
    assert "toString(peer_properties.fork_digest) IN ({digests:Array(String)})" in sql
    assert "toString(peer_properties.next_fork_version) LIKE '%064%'" in sql
    # the old, expensive shape must be gone
    assert "JSONExtractString" not in sql
    assert "toString(peer_properties)" not in sql.replace("toString(peer_properties.", "")


def test_wrap_work_list_adds_anti_join_and_limit_placeholder():
    sql = wrap_work_list("SELECT x AS ip FROM t;", "crawlers_data", "ipinfo")
    assert sql.startswith("SELECT DISTINCT ip FROM (")
    assert "SELECT x AS ip FROM t\n)" in sql
    assert "WHERE ip != '' AND ip NOT IN (SELECT ip FROM crawlers_data.ipinfo)" in sql
    assert sql.rstrip().endswith("LIMIT {max_ips:UInt32}")
    assert sql.count(";") == 0


def test_params_format_millisecond_utc_strings_and_list():
    params = nebula_params(SINCE, UNTIL, DIGESTS)
    assert params == {
        "since": "2026-09-05 12:00:00.123",
        "until": "2026-09-07 12:00:00.000",
        "digests": DIGESTS,
    }
    assert isinstance(params["digests"], list)


def test_ch_datetime64_converts_offsets_to_utc():
    from datetime import timedelta
    tz = timezone(timedelta(hours=2))
    assert ch_datetime64(datetime(2026, 9, 7, 14, 0, 0, tzinfo=tz)) == "2026-09-07 12:00:00.000"


def test_worklist_settings_are_the_agreed_guardrails():
    assert WORKLIST_SETTINGS["max_threads"] == 2
    assert WORKLIST_SETTINGS["max_execution_time"] == 600
    assert WORKLIST_SETTINGS["max_memory_usage"] == 1024 ** 3
    assert WORKLIST_SETTINGS["log_comment"]
    assert RETRYABLE_CH_CODES == {241, 159, 160, 202}


@pytest.mark.parametrize("sql", [
    "SELECT DISTINCT announced_ip AS ip FROM dbt.int_hopr_nodes",
    "  with x as (select 1) select ip from x ; ",
])
def test_validate_readonly_select_accepts_select_and_with(sql):
    cleaned = validate_readonly_select(sql)
    assert not cleaned.endswith(";")


@pytest.mark.parametrize("sql", [
    "INSERT INTO t VALUES (1)",
    "SELECT 1; DROP TABLE t",
    "SELECT * FROM t; SELECT 2",
    "select 1 union all select 2 ; drop table x",
    "SELECT ip FROM t WHERE x = 'a' -- alter table",
    "DESCRIBE t",
])
def test_validate_readonly_select_rejects_non_select_or_multi_statement(sql):
    with pytest.raises(ValueError):
        validate_readonly_select(sql)


def test_dedupe_keep_order_strips_and_drops_empties():
    assert dedupe_keep_order([" 1.1.1.1 ", "", "2.2.2.2", "1.1.1.1", None, "3.3.3.3"]) == [
        "1.1.1.1", "2.2.2.2", "3.3.3.3",
    ]


def test_build_summary_has_stable_keys_and_defaults():
    s = build_summary(source="nebula", mode="once", candidates=3, saved_ok=2, lookup_failed=1, looked_up=3)
    assert list(s)[0] == "event" and s["event"] == "run_summary"
    assert s["source"] == "nebula"
    assert s["truncated"] is False and s["dry_run"] is False
    assert s["saved_failed"] == 0 and s["skipped_existing"] == 0
    assert s["window"] is None
    with pytest.raises(KeyError):
        build_summary(bogus=1)


def test_exit_code_rules():
    ok = build_summary(source="nebula", mode="once", looked_up=5, saved_ok=5)
    assert exit_code_for(ok) == 0
    assert exit_code_for(ok, fatal=True) == 1
    small_all_failed = build_summary(source="nebula", mode="once", looked_up=MIN_ATTEMPTED_FOR_TOTAL_FAILURE - 1, saved_ok=0)
    assert exit_code_for(small_all_failed) == 0
    big_all_failed = build_summary(source="nebula", mode="once", looked_up=MIN_ATTEMPTED_FOR_TOTAL_FAILURE, saved_ok=0)
    assert exit_code_for(big_all_failed) == 1
    partial = build_summary(source="nebula", mode="once", looked_up=100, saved_ok=1, lookup_failed=99)
    assert exit_code_for(partial) == 0
    truncated = build_summary(source="nebula", mode="once", looked_up=10, saved_ok=10, truncated=True)
    assert exit_code_for(truncated) == 0
