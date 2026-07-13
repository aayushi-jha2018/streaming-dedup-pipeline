"""End-to-end test against a real Kafka broker: produce events (with
intentional duplicates) and confirm the consumer's dedup + windowing
results match what the unique input set should produce.

Requires a Kafka broker reachable at KAFKA_BOOTSTRAP_SERVERS (defaults to
localhost:9092); the CI workflow provides this via a service container.
"""

import os
import uuid

from pipeline.producer import run as produce
from pipeline.consumer import run as consume

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def test_producer_consumer_roundtrip_dedups_and_aggregates():
    topic = f"page-views-test-{uuid.uuid4().hex[:8]}"

    sent = produce(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=topic,
        count=50,
        duplicate_rate=0.2,
        seed=7,
    )
    assert sent > 50  # duplicates were actually injected

    result = consume(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=topic,
        window_size_seconds=10,
        ttl_seconds=300,
        consumer_timeout_ms=15000,
    )

    assert result["total_messages"] == sent
    assert result["duplicate_messages"] == sent - 50
    assert result["unique_events"] == 50
    assert sum(result["windows"].values()) == 50
