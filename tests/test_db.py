import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

from src.sources import WORKLIST_SETTINGS
from src.utils import sanitize_ip_info
from tests.conftest import FakeQueryResult


def test_fetch_work_list_binds_params_settings_and_limit_plus_one(db, ch_client):
    ch_client.query.return_value = FakeQueryResult([("1.1.1.1",), ("2.2.2.2",), ("1.1.1.1",)])
    ips, truncated = db.fetch_work_list("SELECT x AS ip FROM t", {"since": "a"}, max_ips=10)
    assert ips == ["1.1.1.1", "2.2.2.2"] and truncated is False
    call = ch_client.query.call_args
    sql = call.args[0] if call.args else call.kwargs["query"]
    assert "NOT IN (SELECT ip FROM crawlers_data.ipinfo)" in sql
    assert call.kwargs["parameters"] == {"since": "a", "max_ips": 11}
    assert call.kwargs["settings"] == WORKLIST_SETTINGS


def test_fetch_work_list_reports_truncation_when_limit_plus_one_rows_return(db, ch_client):
    ch_client.query.return_value = FakeQueryResult([(f"10.0.0.{i}",) for i in range(4)])
    ips, truncated = db.fetch_work_list("SELECT x AS ip FROM t", {}, max_ips=3)
    assert len(ips) == 3 and truncated is True


def test_fetch_work_list_retries_once_on_memory_limit_then_raises(db, ch_client, monkeypatch):
    from src import db as db_module
    from src.db import WorkListError

    sleeps = []
    monkeypatch.setattr(db_module.time, "sleep", lambda s: sleeps.append(s))
    ch_client.query.side_effect = DatabaseError("Code: 241. DB::Exception: (total) memory limit exceeded")
    with pytest.raises(WorkListError):
        db.fetch_work_list("SELECT x AS ip FROM t", {}, max_ips=5)
    assert ch_client.query.call_count == 2
    assert sleeps == [60]


def test_fetch_work_list_recovers_when_retry_succeeds(db, ch_client, monkeypatch):
    from src import db as db_module

    monkeypatch.setattr(db_module.time, "sleep", lambda s: None)
    ch_client.query.side_effect = [
        DatabaseError("Code: 159. DB::Exception: Timeout exceeded"),
        FakeQueryResult([("9.9.9.9",)]),
    ]
    ips, truncated = db.fetch_work_list("SELECT x AS ip FROM t", {}, max_ips=5)
    assert ips == ["9.9.9.9"] and truncated is False


def test_fetch_work_list_non_retryable_error_fails_immediately(db, ch_client, monkeypatch):
    from src import db as db_module
    from src.db import WorkListError

    monkeypatch.setattr(db_module.time, "sleep", lambda s: pytest.fail("must not sleep"))
    ch_client.query.side_effect = DatabaseError("Code: 62. DB::Exception: Syntax error")
    with pytest.raises(WorkListError):
        db.fetch_work_list("SELECT x AS ip FROM t", {}, max_ips=5)
    assert ch_client.query.call_count == 1


def test_ip_exists_uses_bound_parameter(db, ch_client):
    ch_client.query.return_value = FakeQueryResult([(1,)])
    assert db.ip_exists("1.1.1.1'; DROP TABLE x; --") is True
    call = ch_client.query.call_args
    sql = call.args[0] if call.args else call.kwargs["query"]
    assert "{ip:String}" in sql and "DROP" not in sql
    assert call.kwargs["parameters"] == {"ip": "1.1.1.1'; DROP TABLE x; --"}
    ch_client.query.return_value = FakeQueryResult([])
    assert db.ip_exists("2.2.2.2") is False


def test_save_ip_info_writes_flattened_company_carrier_abuse_and_flags(db, ch_client, raw_payload):
    db.save_ip_info(sanitize_ip_info(raw_payload))
    ch_client.insert.assert_called_once()
    call = ch_client.insert.call_args
    table = call.args[0] if call.args else call.kwargs["table"]
    assert table == "crawlers_data.ipinfo"
    rows = call.args[1] if len(call.args) > 1 else call.kwargs["data"]
    cols = call.kwargs["column_names"]
    row = dict(zip(cols, rows[0]))
    assert row["ip"] == "8.8.8.8"
    assert row["company"] == "Google LLC"
    assert row["carrier"] == "T-Mobile"
    assert row["abuse_email"] == "network-abuse@google.com"
    assert row["abuse_phone"] == "+1-650-253-0000"
    assert row["is_bogon"] is False and row["is_mobile"] is True
    assert row["success"] is True and row["error"] == ""


def test_save_ip_info_failure_row(db, ch_client):
    db.save_ip_info({"ip": "5.5.5.5"}, success=False, error="API error: 404")
    row = dict(zip(ch_client.insert.call_args.kwargs["column_names"], ch_client.insert.call_args.args[1][0]))
    assert row["ip"] == "5.5.5.5" and row["success"] is False and row["error"] == "API error: 404"
    assert row["company"] == "" and row["is_mobile"] is False
