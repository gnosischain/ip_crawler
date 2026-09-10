import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
import requests

from src import crawler as crawler_module
from src.crawler import IPInfoCrawler, RateLimiter
from src.sources import MIN_ATTEMPTED_FOR_TOTAL_FAILURE

NOW = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, bad_json=False):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}
        self.text = json.dumps(self._payload)
        self._bad_json = bad_json

    def json(self):
        if self._bad_json:
            raise ValueError("not json")
        return self._payload


@pytest.fixture
def fake_db():
    db = MagicMock(name="db")
    db.ip_exists.return_value = False
    db.fetch_work_list.return_value = ([], False)
    return db


@pytest.fixture
def crawler(fake_db, monkeypatch, tmp_path):
    monkeypatch.setattr(crawler_module, "LOG_PATH", str(tmp_path))
    return IPInfoCrawler(db=fake_db, fork_digests=["0xabc"], rate_limiter=RateLimiter(0))


def install_http(monkeypatch, responses):
    calls = []

    def fake_get(url, headers=None, timeout=None):
        calls.append(url)
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(crawler_module.requests, "get", fake_get)
    return calls


def test_rate_limiter_spaces_calls(monkeypatch):
    clock = [100.0]
    sleeps = []
    rl = RateLimiter(2.0, sleep=lambda s: sleeps.append(s) or clock.__setitem__(0, clock[0] + s), clock=lambda: clock[0])
    rl.wait(); rl.wait(); clock[0] += 0.5; rl.wait()
    assert sleeps == [2.0, 1.5]


def test_process_ip_success_saves_sanitized_row(crawler, fake_db, monkeypatch, raw_payload):
    install_http(monkeypatch, [FakeResponse(200, raw_payload)])
    assert crawler.process_ip("8.8.8.8") == "saved"
    saved = fake_db.save_ip_info.call_args.args[0]
    assert saved["company"] == "Google LLC" and saved["abuse_email"] == "network-abuse@google.com"
    assert fake_db.save_ip_info.call_args.kwargs.get("success", True) is True


def test_process_ip_skips_when_row_appeared_since_worklist(crawler, fake_db, monkeypatch):
    fake_db.ip_exists.return_value = True
    calls = install_http(monkeypatch, [])
    assert crawler.process_ip("1.1.1.1") == "skipped_existing"
    assert calls == [] and not fake_db.save_ip_info.called


def test_process_ip_404_persists_failure_row(crawler, fake_db, monkeypatch):
    install_http(monkeypatch, [FakeResponse(404, {"error": "nope"})])
    assert crawler.process_ip("2.2.2.2") == "saved_failed"
    kwargs = fake_db.save_ip_info.call_args.kwargs
    assert kwargs["success"] is False and "404" in kwargs["error"]


def test_process_ip_timeout_is_transient_not_persisted(crawler, fake_db, monkeypatch):
    install_http(monkeypatch, [requests.Timeout("t"), requests.Timeout("t"), requests.Timeout("t")])
    assert crawler.process_ip("3.3.3.3") == "lookup_failed"
    assert not fake_db.save_ip_info.called


def test_process_ip_5xx_is_transient_and_retried(crawler, fake_db, monkeypatch, raw_payload):
    calls = install_http(monkeypatch, [FakeResponse(503, {}), FakeResponse(200, raw_payload)])
    assert crawler.process_ip("8.8.8.8") == "saved"
    assert len(calls) == 2


def test_process_ip_401_is_fatal(crawler, fake_db, monkeypatch):
    from src.crawler import AuthError
    install_http(monkeypatch, [FakeResponse(401, {})])
    with pytest.raises(AuthError):
        crawler.process_ip("4.4.4.4")


def test_run_work_list_counts_and_summary(crawler, fake_db, monkeypatch, raw_payload):
    install_http(monkeypatch, [
        FakeResponse(200, raw_payload),
        FakeResponse(404, {}),
        requests.Timeout("t"), requests.Timeout("t"), requests.Timeout("t"),
    ])
    summary = crawler.run_work_list(["8.8.8.8", "2.2.2.2", "3.3.3.3"], source="test", mode="once")
    assert summary["event"] == "run_summary"
    assert summary["candidates"] == 3 and summary["looked_up"] == 3
    assert summary["saved_ok"] == 1 and summary["saved_failed"] == 1 and summary["lookup_failed"] == 1
    assert summary["exit_code"] == 0


def test_run_work_list_stops_on_shutdown_and_still_returns_summary(crawler, fake_db, monkeypatch, raw_payload):
    def stop_after_first(url, headers=None, timeout=None):
        crawler.running = False
        return FakeResponse(200, raw_payload)
    monkeypatch.setattr(crawler_module.requests, "get", stop_after_first)
    summary = crawler.run_work_list(["8.8.8.8", "9.9.9.9", "7.7.7.7"], source="test", mode="once")
    assert summary["saved_ok"] == 1 and summary["skipped_shutdown"] == 2


def test_dry_run_makes_no_http_and_no_inserts(crawler, fake_db, monkeypatch, tmp_path):
    calls = install_http(monkeypatch, [])
    crawler.dry_run = True
    summary = crawler.run_work_list(["1.1.1.1", "2.2.2.2"], source="test", mode="dry_run")
    assert calls == [] and not fake_db.save_ip_info.called
    assert summary["candidates"] == 2 and summary["looked_up"] == 0 and summary["dry_run"] is True
    assert (tmp_path / "worklist.txt").read_text().split() == ["1.1.1.1", "2.2.2.2"]


def test_run_nebula_window_queries_each_chunk_and_caps(crawler, fake_db, monkeypatch):
    monkeypatch.setattr(crawler_module, "MAX_IPS_PER_RUN", 3)
    monkeypatch.setattr(crawler_module, "WINDOW_CHUNK_HOURS", 24)
    fake_db.fetch_work_list.side_effect = [(["a", "b"], False), (["b", "c", "d"], True)]
    crawler.dry_run = True
    summary = crawler.run_nebula_window(NOW - timedelta(days=2), NOW)
    assert fake_db.fetch_work_list.call_count == 2
    assert summary["candidates"] == 3 and summary["truncated"] is True
    assert summary["window"]["chunks"] == 2 and summary["window"]["since"].startswith("2026-09-05")


def test_total_failure_exit_code(crawler, fake_db, monkeypatch):
    n = MIN_ATTEMPTED_FOR_TOTAL_FAILURE
    install_http(monkeypatch, [requests.ConnectionError("x")] * (n * 3))
    summary = crawler.run_work_list([f"10.0.0.{i}" for i in range(n)], source="test", mode="once")
    assert summary["looked_up"] == n and summary["saved_ok"] == 0 and summary["exit_code"] == 1


# --- two-phase nightly run ------------------------------------------------------
import itertools  # noqa: E402

from src.db import WorkListError  # noqa: E402
from src.sources import ch_datetime64  # noqa: E402


def _set_windows(monkeypatch, *, lookback=2, sweep=4, sweep_cap=3, chunk_hours=24, sweep_seconds=3600):
    monkeypatch.setattr(crawler_module, "LOOKBACK_DAYS", lookback)
    monkeypatch.setattr(crawler_module, "SWEEP_LOOKBACK_DAYS", sweep)
    monkeypatch.setattr(crawler_module, "SWEEP_MAX_IPS_PER_RUN", sweep_cap)
    monkeypatch.setattr(crawler_module, "SWEEP_MAX_SECONDS", sweep_seconds)
    monkeypatch.setattr(crawler_module, "WINDOW_CHUNK_HOURS", chunk_hours)
    monkeypatch.setattr(crawler_module, "MAX_IPS_PER_RUN", 100)


def test_run_nightly_recent_then_sweep_oldest_first_under_cap(crawler, fake_db, monkeypatch):
    _set_windows(monkeypatch)
    crawler.dry_run = True
    fake_db.fetch_work_list.side_effect = [(["r1"], False), (["r2"], False), (["a", "b"], False), (["c", "d"], True)]
    summary = crawler.run_nightly(now=NOW)
    assert fake_db.fetch_work_list.call_count == 4                       # 2 recent chunks + 2 sweep chunks
    sweep_first_call = fake_db.fetch_work_list.call_args_list[2]
    assert sweep_first_call.args[1]["since"] == ch_datetime64(NOW - timedelta(days=4))   # oldest first
    assert sweep_first_call.args[2] == 3                                  # sweep cap, not MAX_IPS_PER_RUN
    assert summary["phases"]["recent"]["candidates"] == 2
    assert summary["phases"]["sweep"]["candidates"] == 3 and summary["phases"]["sweep"]["truncated"] is True
    assert summary["candidates"] == 5 and summary["truncated"] is False   # top level = recent-phase truncation
    assert summary["phases"]["sweep"]["window"]["until"] == (NOW - timedelta(days=2)).isoformat()
    assert summary["exit_code"] == 0


def test_run_nightly_sweep_disabled(crawler, fake_db, monkeypatch):
    _set_windows(monkeypatch, sweep=0)
    crawler.dry_run = True
    fake_db.fetch_work_list.side_effect = [(["r1"], False), (["r2"], False)]
    summary = crawler.run_nightly(now=NOW)
    assert fake_db.fetch_work_list.call_count == 2
    assert summary["phases"]["sweep"] is None and summary["phases"]["recent"]["candidates"] == 2


def test_run_nightly_sweep_time_budget_stops_and_still_summarises(crawler, fake_db, monkeypatch, raw_payload):
    _set_windows(monkeypatch, lookback=1, sweep=2, sweep_cap=50, sweep_seconds=10)
    clock = itertools.count(0, 4)                                          # every monotonic() call advances 4 s
    monkeypatch.setattr(crawler_module.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(crawler_module.requests, "get", lambda url, headers=None, timeout=None: FakeResponse(200, raw_payload))
    fake_db.fetch_work_list.side_effect = [(["r1"], False), ([f"10.0.0.{i}" for i in range(20)], False)]
    summary = crawler.run_nightly(now=NOW)
    sweep = summary["phases"]["sweep"]
    assert summary["phases"]["recent"]["saved_ok"] == 1                    # recent phase has no deadline
    assert sweep["skipped_shutdown"] >= 1 and sweep["budget_exhausted"] is True
    assert sweep["saved_ok"] + sweep["skipped_shutdown"] == sweep["candidates"]
    assert crawler.running is True and summary["exit_code"] == 0


def test_run_nightly_sweep_worklist_error_is_not_fatal(crawler, fake_db, monkeypatch, raw_payload):
    _set_windows(monkeypatch, lookback=1, sweep=2)
    monkeypatch.setattr(crawler_module.requests, "get", lambda url, headers=None, timeout=None: FakeResponse(200, raw_payload))
    fake_db.fetch_work_list.side_effect = [(["r1"], False), WorkListError("boom")]
    summary = crawler.run_nightly(now=NOW)
    assert summary["exit_code"] == 0 and summary["saved_ok"] == 1
    assert "boom" in summary["phases"]["sweep"]["error"] and summary["clickhouse_errors"] == 1


def test_dry_run_nightly_writes_both_worklists(crawler, fake_db, monkeypatch, tmp_path):
    _set_windows(monkeypatch, lookback=1, sweep=2)
    crawler.dry_run = True
    calls = install_http(monkeypatch, [])
    fake_db.fetch_work_list.side_effect = [(["a"], False), (["b"], False)]
    summary = crawler.run_nightly(now=NOW)
    assert calls == [] and summary["dry_run"] is True
    assert (tmp_path / "worklist.txt").read_text().split() == ["a"]
    assert (tmp_path / "worklist_sweep.txt").read_text().split() == ["b"]


def test_run_continuous_uses_run_nightly(crawler, monkeypatch):
    calls = []
    def fake_nightly(mode="once"):
        calls.append(mode); crawler.running = False
        return {"event": "run_summary", "exit_code": 0}
    monkeypatch.setattr(crawler, "run_nightly", fake_nightly)
    monkeypatch.setattr(crawler_module, "emit_summary", lambda s: None)
    crawler.run_continuous()
    assert calls == ["continuous"]


def test_main_dispatch_once_vs_explicit_window(monkeypatch, tmp_path):
    monkeypatch.setattr(crawler_module, "LOG_PATH", str(tmp_path))
    seen = []
    class StubCrawler:
        def __init__(self, dry_run=False): pass
        def run_nightly(self): seen.append("nightly"); return {"event": "run_summary", "exit_code": 0}
        def run_nebula_window(self, since, until): seen.append(("window", since.date().isoformat(), until.date().isoformat())); return {"event": "run_summary", "exit_code": 0}
    monkeypatch.setattr(crawler_module, "IPInfoCrawler", StubCrawler)
    assert crawler_module.main(["--once"]) == 0
    assert crawler_module.main(["--since", "2026-08-01", "--until", "2026-08-03"]) == 0
    assert crawler_module.main(["--once", "--lookback-days", "5"]) == 0
    assert seen[0] == "nightly" and seen[1] == ("window", "2026-08-01", "2026-08-03") and seen[2][0] == "window"
