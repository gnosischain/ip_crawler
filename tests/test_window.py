from datetime import datetime, timedelta, timezone

import pytest

from src.sources import chunk_window, compute_window, parse_iso_utc

NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


def test_default_window_is_lookback_days_ending_now():
    since, until = compute_window(NOW, 2)
    assert until == NOW
    assert since == NOW - timedelta(days=2)


def test_since_only_keeps_until_at_now():
    since, until = compute_window(NOW, 2, since="2026-09-01")
    assert since == datetime(2026, 9, 1, tzinfo=timezone.utc)
    assert until == NOW


def test_until_only_backs_off_lookback_from_until():
    since, until = compute_window(NOW, 30, until="2026-09-01T00:00:00Z")
    assert until == datetime(2026, 9, 1, tzinfo=timezone.utc)
    assert since == datetime(2026, 8, 2, tzinfo=timezone.utc)


def test_explicit_both_bounds_win_over_lookback():
    since, until = compute_window(NOW, 2, since="2026-07-01", until="2026-08-01")
    assert (since, until) == (
        datetime(2026, 7, 1, tzinfo=timezone.utc),
        datetime(2026, 8, 1, tzinfo=timezone.utc),
    )


def test_naive_iso_is_utc_and_offsets_are_normalised():
    assert parse_iso_utc("2026-09-07T10:00:00") == datetime(2026, 9, 7, 10, tzinfo=timezone.utc)
    assert parse_iso_utc("2026-09-07T12:00:00+02:00") == datetime(2026, 9, 7, 10, tzinfo=timezone.utc)


def test_empty_or_inverted_window_raises():
    with pytest.raises(ValueError):
        compute_window(NOW, 2, since="2026-09-08")
    with pytest.raises(ValueError):
        compute_window(NOW, 2, since="2026-09-07T12:00:00Z")


def test_chunk_window_is_half_open_24h_chunks_with_short_tail():
    since = NOW - timedelta(hours=50)
    chunks = chunk_window(since, NOW, 24)
    assert len(chunks) == 3
    assert chunks[0] == (since, since + timedelta(hours=24))
    assert chunks[1] == (since + timedelta(hours=24), since + timedelta(hours=48))
    assert chunks[2] == (since + timedelta(hours=48), NOW)
    assert chunks[2][1] - chunks[2][0] == timedelta(hours=2)
    # contiguous, no gaps or overlaps
    for (_, a_end), (b_start, _) in zip(chunks, chunks[1:]):
        assert a_end == b_start


def test_thirty_day_window_gives_thirty_daily_chunks():
    since, until = compute_window(NOW, 30)
    assert len(chunk_window(since, until, 24)) == 30


def test_chunk_hours_zero_means_single_chunk():
    since, until = compute_window(NOW, 5)
    assert chunk_window(since, until, 0) == [(since, until)]
