# streaming-dedup-pipeline
A Kafka streaming pipeline demo: a producer that simulates at-least-once delivery (with duplicates), and a consumer that deduplicates events and aggregates counts into tumbling time windows, with a real integration test against a live Kafka broker in CI.
