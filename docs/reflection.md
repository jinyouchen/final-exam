1.
Impact: Raw data is stored but no curated data is written, causing data loss in the curated layer and duplicate raw data risk on re-run.
Solution: Use Spark Structured Streaming checkpointing with write-ahead logs (WAL) and transactional commits to the curated zone to ensure exactly-once semantics.
2.
Bottlenecks: 1. Insufficient Kafka topic partitions limiting parallelism. 2. Spark consumer parallelism (executor cores) not matching partition count. 3. Slow data lake write throughput.
Fixes: Increase Kafka partitions, scale Spark executors to match partition count, optimize producer batching, and use partitioned writes to the data lake.
3.
Kafka: Pros - low-latency recent data access, built-in stream processing. Cons - limited retention, high long-term storage cost, poor for analytics. Preferred for real-time use cases.
Parquet Lake: Pros - efficient compressed storage, optimized for analytics, low-cost long-term retention. Cons - higher latency, not for low-latency reads. Preferred for batch/historical analytics.
4.
Detection: Spark-based anomaly detection (threshold checks for sensor value ranges, or statistical methods like z-scores).
Isolation: Flag records with an is_anomaly field in the curated Parquet data, or write them to a dedicated "anomalies" zone in the data lake without deletion.
5.
Spark ETL code: Update anomaly detection rules to include CO₂ value ranges.
Flask API: Ensure endpoints (/api/sensors, stats/latest) support CO₂ sensor data.
(Optional) Documentation: Update README.md and docs/ to include CO₂ sensor details.
No changes needed for Kafka if the data schema is generic.