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
