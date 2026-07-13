"""Tumbling-window aggregation over event-time timestamps.

Aggregation buckets events by their embedded event_time (not wall-clock
arrival time), so results are reproducible regardless of processing delay
or the order messages happen to arrive in -- a common requirement in real
stream-processing systems (Kafka Streams, Flink, Spark Structured
Streaming) where "event time" and "processing time" are treated as
distinct concepts.
"""

from collections import defaultdict
from typing import Dict, Tuple


class TumblingWindowAggregator:
    """Counts events per (window_start, key) using fixed-size, non-overlapping windows."""

    def __init__(self, window_size_seconds: int = 10):
        if window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be positive")
        self.window_size_seconds = window_size_seconds
        self._counts: Dict[Tuple[int, str], int] = defaultdict(int)

    def add(self, key: str, event_time: float) -> None:
        window_start = int(event_time - (event_time % self.window_size_seconds))
        self._counts[(window_start, key)] += 1

    def results(self) -> Dict[Tuple[int, str], int]:
        return dict(self._counts)
