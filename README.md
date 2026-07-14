# streaming-dedup-pipeline

A Kafka streaming pipeline demo: a producer that simulates at-least-once delivery (with duplicates), and a consumer that deduplicates events and aggregates counts into tumbling time windows. Companion piece to agentic-research-assistant -- that one's about agentic orchestration, this one's about the dedup/windowing problems that show up whenever something consumes from a message queue.

## Run it locally

```
docker compose up -d
pip install -r requirements.txt
python -m pipeline.producer --count 100 --duplicate-rate 0.15
python -m pipeline.consumer
python -m pytest tests/ -v
```

Run these as modules (`python -m pipeline.producer`, not `python pipeline/producer.py`) -- the internal imports (`from pipeline.dedup import ...`) need the package resolvable from the repo root.

## Why simulate duplicates instead of assuming exactly-once delivery?

Kafka's default delivery guarantee is at-least-once: if a producer doesn't get an acknowledgment in time (a network blip, a broker failover), it retries, and the same event can land in the topic twice. Idempotent producers and transactional writes reduce this, but consumers of real-world topics still need to handle duplicates defensively. The producer here intentionally resends 10-20% of events with the same event_id to exercise that path, rather than assuming a happy path that production data never actually gives you.

## How it flows

```
producer.py -> Kafka topic "page-views" -> consumer.py
                                              |
                                              v
                                       Deduplicator (TTL-based, keyed on event_id)
                                              |
                                              v
                                    TumblingWindowAggregator (buckets by event_time, not wall-clock)
                                              |
                                              v
                                    {window_start, page}: count
```

## Design notes

I went with tumbling windows (fixed, non-overlapping buckets) rather than sliding windows mostly for simplicity -- the aggregation logic and the tests are a lot easier to reason about when each event belongs to exactly one window. The real cost of that choice shows up at the edges: an event that arrives a few seconds into the next window doesn't get folded back into the previous one. A production version of this would likely need either a small allowed-lateness grace period, or an actual sliding/hopping window if the business question genuinely needs overlapping counts.

The dedup store is also intentionally naive: an in-memory TTL cache keyed on event_id, sized for a demo's worth of events. A real deployment consuming a high-volume topic would need this backed by something like Redis or a compacted Kafka topic -- an in-memory set doesn't survive a consumer restart, and it doesn't work at all once there's more than one consumer instance in a group.

## Layout

- `pipeline/producer.py` -- simulates page-view events, injects duplicates
- `pipeline/consumer.py` -- consumes, dedups, aggregates into windows
- `pipeline/dedup.py` -- Deduplicator: TTL-based seen-event tracking
- `pipeline/windowing.py` -- TumblingWindowAggregator: event-time bucketing
- `scripts/wait_for_kafka.py` -- polls broker readiness, used in CI
- `tests/test_pipeline.py` -- unit tests, no Kafka needed
- `tests/test_integration.py` -- real producer -> real Kafka -> real consumer
- `docker-compose.yml` -- single-node KRaft Kafka broker for local runs

## CI

Two kinds of tests run on every push: fast unit tests for the dedup/windowing logic with no dependencies, and a real integration test where CI spins up an actual Kafka broker (KRaft mode, no Zookeeper) as a service container and runs the full producer-to-consumer path against it.

## In production

This would run on managed Kafka (MSK or Confluent Cloud), with the consumer as a long-running service instead of a batch script, and the window output landing in a warehouse or a downstream topic instead of stdout -- see dbt-snowflake-elt-demo for the warehouse side of that pattern.

MIT license.
