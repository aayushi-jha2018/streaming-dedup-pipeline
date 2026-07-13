"""Deduplication logic for at-least-once delivery semantics.

Kafka producers configured for at-least-once delivery (the common case when
a producer retries after a network blip without idempotence enabled) can
publish the same logical event more than once. This module tracks event IDs
that have already been processed within a trailing time-to-live window and
flags repeats so downstream aggregation only counts each event once.
"""

from typing import Dict


class Deduplicator:
    """Tracks seen event IDs within a trailing TTL window.

    A plain dict is enough for a demo of this size; a production system
    processing millions of events per window would back this with Redis
    or a bounded LRU cache instead of an unbounded-until-pruned dict.
    """

    def __init__(self, ttl_seconds: int = 300):
        self.ttl_seconds = ttl_seconds
        self._expiry_by_id: Dict[str, float] = {}

    def is_duplicate(self, event_id: str, now: float) -> bool:
        """Return True if event_id was already seen and hasn't expired yet."""
        self._prune(now)
        if event_id in self._expiry_by_id:
            return True
        self._expiry_by_id[event_id] = now + self.ttl_seconds
        return False

    def _prune(self, now: float) -> None:
        expired_ids = [eid for eid, expiry in self._expiry_by_id.items() if expiry <= now]
        for eid in expired_ids:
            del self._expiry_by_id[eid]

    def __len__(self) -> int:
        return len(self._expiry_by_id)
