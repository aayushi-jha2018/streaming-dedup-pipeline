"""Consumes page-view events, deduplicates them, and aggregates counts into
tumbling windows.
"""

import argparse
import json

from kafka import KafkaConsumer

from pipeline.dedup import Deduplicator
from pipeline.windowing import TumblingWindowAggregator


def run(bootstrap_servers: str, topic: str, window_size_seconds: int, ttl_seconds: int, consumer_timeout_ms: int):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=consumer_timeout_ms,
    )

    dedup = Deduplicator(ttl_seconds=ttl_seconds)
    aggregator = TumblingWindowAggregator(window_size_seconds=window_size_seconds)

    total_messages = 0
    duplicate_messages = 0

    for message in consumer:
        event = message.value
        total_messages += 1
        if dedup.is_duplicate(event["event_id"], event["event_time"]):
            duplicate_messages += 1
            continue
        aggregator.add(event["page"], event["event_time"])

    consumer.close()
    return {
        "total_messages": total_messages,
        "duplicate_messages": duplicate_messages,
        "unique_events": total_messages - duplicate_messages,
        "windows": aggregator.results(),
    }


def main():
    parser = argparse.ArgumentParser(description="Consume, dedup, and aggregate page-view events from Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="page-views")
    parser.add_argument("--window-size-seconds", type=int, default=10)
    parser.add_argument("--ttl-seconds", type=int, default=300)
    parser.add_argument("--consumer-timeout-ms", type=int, default=10000)
    args = parser.parse_args()

    result = run(
        args.bootstrap_servers,
        args.topic,
        args.window_size_seconds,
        args.ttl_seconds,
        args.consumer_timeout_ms,
    )

    print(f"Consumed {result['total_messages']} messages, dropped {result['duplicate_messages']} duplicates, "
          f"{result['unique_events']} unique events.")
    for (window_start, page), count in sorted(result["windows"].items()):
        print(f"  window={window_start} page={page} count={count}")


if __name__ == "__main__":
    main()
