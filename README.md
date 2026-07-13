# streaming-dedup-pipeline

A Kafka streaming pipeline demo: a producer that simulates at-least-once
delivery (with duplicates), and a consumer that deduplicates events and
aggregates counts into tumbling time windows.

This is a companion piece to [agentic-research-assistant](https://github.com/aayushi-jha2018/agentic-research-assistant): where that repo demonstrates
agentic orchestration, this one demonstrates streaming data engineering --
the kind of at-least-once delivery, deduplication, and event-time windowing
problems that come up whenever a real system consumes from a message queue.

## Why simulate duplicates instead of assuming exactly-once delivery?

Kafka's default delivery guarantee is at-least-once: if a producer doesn't
get an acknowledgment in time (a network blip, a broker failover), it
retries, and the same event can land in the topic twice. Idempotent
producers and transactional writes can reduce this, but consumers of
real-world topics still need to handle duplicates defensively. The producer
here intentionally resends ~10-20% of events with the same `event_id` to
exercise that path, rather than assuming a happy path that production data
never gives you.

## Architecture

```
producer.py --> Kafka topic "page-views" --> consumer.py
                                                |
                                                v
                                       Deduplicator (TTL-based,
                                       keyed on event_id)
                                                |
                                                v
                                       TumblingWindowAggregator
                                       (buckets by event_time,
                                        not wall-clock time)
                                                |
                                                v
                                       {window_start, page}: count
```

## Project structure

```
streaming-dedup-pipeline/
|-- pipeline/
|   |-- producer.py    # simulates page-view events, injects duplicates
|   |-- consumer.py    # consumes, dedups, aggregates into windows
|   |-- dedup.py       # Deduplicator: TTL-based seen-event tracking
|   `-- windowing.py   # TumblingWindowAggregator: event-time bucketing
|-- scripts/
|   `-- wait_for_kafka.py  # polls broker readiness, used in CI
|-- tests/
|   |-- test_pipeline.py     # unit tests, no Kafka needed
|   `-- test_integration.py  # real producer -> real Kafka -> real consumer
|-- docker-compose.yml  # single-node KRaft Kafka broker for local runs
`-- pytest.ini
```

## Running this project

Locally, with Docker:

```
docker compose up -d
pip install -r requirements.txt
python -m pipeline.producer --count 100 --duplicate-rate 0.15
python -m pipeline.consumer
python -m pytest tests/ -v
```

Note the `-m` flag: these modules use `from pipeline.dedup import ...`-style
imports, so they need to be run as modules from the repo root, not as bare
scripts (`python pipeline/producer.py` would fail to resolve the import).

## What this demonstrates

- **At-least-once delivery handling**: the producer models real Kafka retry
  behavior instead of assuming every message arrives exactly once.
- **Deduplication with bounded memory**: a TTL-based dedup window, the same
  general approach (if not the same backing store) used in production
  systems that can't keep every event ID forever.
- **Event-time windowing**: aggregation is keyed off the timestamp embedded
  in each event, not when the consumer happens to process it, so results
  don't depend on processing delay or message reordering within a window.
- **A real integration test**: CI spins up an actual Kafka broker (KRaft
  mode, no Zookeeper) as a service container and runs the full
  producer-to-consumer path against it, in addition to fast, dependency-free
  unit tests for the core dedup/windowing logic.

In production, this would run on managed Kafka (MSK/Confluent Cloud) with
the consumer as a long-running service rather than a batch script, and the
window output would land in a warehouse or a downstream topic instead of
stdout -- see [dbt-snowflake-elt-demo](https://github.com/aayushi-jha2018/dbt-snowflake-elt-demo) for the warehouse side of that
pattern.

## License

MIT -- feel free to reuse this as a starting point.
