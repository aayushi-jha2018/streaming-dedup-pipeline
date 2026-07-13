"""Simulated page-view event producer.

Publishes JSON-encoded page-view events to a Kafka topic. To model the
at-least-once delivery semantics of a real Kafka producer (where a retry
after a timeout can result in the same event being written twice), this
producer intentionally re-sends a fraction of events with the same
event_id after their first send.
"""

import argparse
import json
import random
import time
import uuid

from kafka import KafkaProducer

PAGES = ["/home", "/pricing", "/docs", "/blog", "/signup"]


def build_event(page: str, event_time: float) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "page": page,
        "event_time": event_time,
    }


def run(bootstrap_servers: str, topic: str, count: int, duplicate_rate: float, seed: int) -> int:
    rng = random.Random(seed)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    start_time = time.time()
    sent = 0
    for i in range(count):
        page = rng.choice(PAGES)
        event_time = start_time + i * 0.1
        event = build_event(page, event_time)
        producer.send(topic, event)
        sent += 1

        if rng.random() < duplicate_rate:
            # Simulate an at-least-once redelivery of the same event.
            producer.send(topic, event)
            sent += 1

    producer.flush()
    producer.close()
    return sent


def main():
    parser = argparse.ArgumentParser(description="Produce simulated page-view events to Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="page-views")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--duplicate-rate", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    sent = run(args.bootstrap_servers, args.topic, args.count, args.duplicate_rate, args.seed)
    print(f"Sent {sent} messages ({args.count} unique events, ~{args.duplicate_rate:.0%} duplicate rate) to '{args.topic}'")


if __name__ == "__main__":
    main()
