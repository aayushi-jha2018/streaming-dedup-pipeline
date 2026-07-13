"""Unit tests for the dedup and windowing logic. No Kafka broker required."""

import pytest

from pipeline.dedup import Deduplicator
from pipeline.windowing import TumblingWindowAggregator


def test_dedup_flags_repeated_event_id():
    dedup = Deduplicator(ttl_seconds=300)
    assert dedup.is_duplicate("evt-1", now=1000) is False
    assert dedup.is_duplicate("evt-1", now=1001) is True
    assert dedup.is_duplicate("evt-2", now=1001) is False


def test_dedup_expires_after_ttl():
    dedup = Deduplicator(ttl_seconds=10)
    assert dedup.is_duplicate("evt-1", now=0) is False
    # After the TTL has elapsed, the same ID is treated as new again.
    assert dedup.is_duplicate("evt-1", now=100) is False


def test_dedup_len_tracks_active_ids():
    dedup = Deduplicator(ttl_seconds=300)
    dedup.is_duplicate("evt-1", now=0)
    dedup.is_duplicate("evt-2", now=0)
    assert len(dedup) == 2


def test_windowing_groups_events_into_correct_buckets():
    aggregator = TumblingWindowAggregator(window_size_seconds=10)
    aggregator.add("/home", event_time=1)
    aggregator.add("/home", event_time=5)
    aggregator.add("/home", event_time=12)
    aggregator.add("/docs", event_time=3)

    results = aggregator.results()
    assert results[(0, "/home")] == 2
    assert results[(10, "/home")] == 1
    assert results[(0, "/docs")] == 1


def test_windowing_rejects_non_positive_window_size():
    with pytest.raises(ValueError):
        TumblingWindowAggregator(window_size_seconds=0)
