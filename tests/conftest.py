"""Test environment: no network, no ClickHouse, no rate-limit sleeps.

Environment is set before any `src.*` import because config values are read at
import time.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("IPINFO_RATE_LIMIT", "0")        # RateLimiter(min_interval=0)
os.environ.setdefault("IPINFO_API_TOKEN", "test-token")
os.environ.setdefault("CLICKHOUSE_HOST", "clickhouse.invalid")
os.environ.setdefault("CLICKHOUSE_DATABASE", "crawlers_data")
os.environ.setdefault("IP_INFO_TABLE", "ipinfo")
os.environ.setdefault("MAX_RETRIES", "2")
os.environ.setdefault("RETRY_DELAY", "0")
os.environ.setdefault("REQUEST_TIMEOUT", "1")
os.environ.setdefault("DRY_RUN", "false")


class FakeQueryResult:
    def __init__(self, rows):
        self.result_rows = rows


@pytest.fixture
def ch_client(monkeypatch):
    """A MagicMock standing in for clickhouse_connect's Client, installed on get_client."""
    import clickhouse_connect

    client = MagicMock(name="clickhouse_client")
    client.query.return_value = FakeQueryResult([])
    client.command.return_value = 1
    monkeypatch.setattr(clickhouse_connect, "get_client", lambda **kwargs: client)
    return client


@pytest.fixture
def db(ch_client):
    from src.db import Database

    return Database()


RAW_IPINFO_PAYLOAD = {
    "ip": "8.8.8.8",
    "hostname": "dns.google",
    "city": "Mountain View",
    "region": "California",
    "country": "US",
    "loc": "37.4056,-122.0775",
    "org": "AS15169 Google LLC",
    "postal": "94043",
    "timezone": "America/Los_Angeles",
    "asn": {"asn": "AS15169", "name": "Google LLC"},
    "company": {"name": "Google LLC", "domain": "google.com", "type": "hosting"},
    "carrier": {"name": "T-Mobile", "mcc": "310", "mnc": "260"},
    "abuse": {"email": "network-abuse@google.com", "phone": "+1-650-253-0000"},
    "bogon": False,
    "mobile": True,
}


@pytest.fixture
def raw_payload():
    return dict(RAW_IPINFO_PAYLOAD)
