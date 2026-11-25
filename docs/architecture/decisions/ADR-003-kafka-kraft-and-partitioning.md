# ADR-003: Apache Kafka 4.3.2 (KRaft) Topology, Partitioning & Hot-Key Strategy

- **Status:** Accepted
- **Scope:** Event streaming backbone, topic design, ordering guarantees, and skew mitigation

## Context
Autonomous vehicle telemetry arrives continuously across heterogeneous signal classes (high-frequency GPS/motion vs. bursty diagnostics/autonomy events vs. low-frequency trip boundaries). Downstream streaming jobs require per-vehicle ordering to compute speed deltas, repeated hard-braking patterns, and connectivity gaps.

## Decision
1. **Runtime:** Pin official **`apache/kafka:4.3.2`** in **KRaft mode** (Apache-2.0, multi-arch `amd64`/`arm64`). ZooKeeper was removed in Kafka 4.x, eliminating an entire failure domain and memory consumer.
2. **Multi-Topic Separation:** Separate 8 topics by domain, retention, and consumer SLA:
   - `fleet.telemetry.raw` (12 partitions, zstd, 7d retention) — canonical vehicle sensor frame
   - `fleet.gps.raw` (12 partitions, zstd, 7d retention) — high-frequency geospatial positions
   - `fleet.diagnostics.raw` (6 partitions, zstd, 14d retention) — DTC fault codes, battery/thermal alerts
   - `fleet.trips.raw` (6 partitions, zstd, 30d retention) — trip start/end lifecycle records
   - `fleet.autonomy.raw` (6 partitions, zstd, 30d retention) — synthetic autonomy disengagement events
   - `fleet.anomalies.detected` (6 partitions, zstd, 30d retention) — Flink-emitted rule anomalies
   - `fleet.vehicle-state` (6 partitions, compacted + delete, 30d retention) — latest keyed vehicle status & gap state
   - `fleet.telemetry.dlq` (3 partitions, zstd, 30d retention) — schema rejections and poison payloads with error metadata
3. **Partition Key Strategy:** Partition vehicle-scoped topics by `vehicle_id` using Kafka's standard `murmur2(key) % num_partitions`.
   - **What this guarantees:** All events for a single `vehicle_id` land in the same partition and preserve broker append order within that partition.
   - **What this does NOT guarantee:** Global ordering across different vehicles or across partitions, nor event-time ordering when a single vehicle reconnects after a cellular outage and flushes buffered out-of-order messages. Event-time ordering is therefore resolved in Flink via watermarks, never assumed from Kafka offsets.
4. **Hot-Key Risk & Mitigation:** If a test/calibration vehicle or malfunctioning telemetry gateway emits 100x normal frequency, partitioning strictly by `vehicle_id` can saturate one Kafka partition and one Flink subtask. Mitigations implemented and documented:
   - Producer-side per-vehicle token-bucket rate limiting and anomaly burst capping.
   - In Spark batch joins/aggregations, skewed keys are mitigated via **salting** (`concat(vehicle_id, '_', salt_bucket)`) and Spark **AQE skew join optimization** (`spark.sql.adaptive.skewJoin.enabled=true`).
   - For extreme streaming hot keys (>5% of cluster throughput from one vehicle), the architecture documents a two-stage salted key pattern (`vehicle_id#shard` for stateless pre-aggregation followed by `vehicle_id` re-keying), noting the tradeoff of losing single-stage per-vehicle sequence continuity.

## Consequences
- Clean operational isolation between high-volume telemetry and critical anomaly/DLQ streams.
- Explicit documentation of ordering limits prevents subtle bugs in stateful stream windows.
