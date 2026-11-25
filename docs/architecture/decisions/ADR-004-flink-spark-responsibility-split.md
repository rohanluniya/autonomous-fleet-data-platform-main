# ADR-004: Responsibility Split Between Apache Flink and Apache Spark

- **Status:** Accepted
- **Scope:** Streaming vs. batch processing boundaries, Iceberg connectors, and sink semantics

## Context
A common anti-pattern in data platforms is forcing Spark micro-batching to handle sub-second stateful CEP alerts, or forcing Flink to perform heavy historical SCD Type 2 merges, backfills, and table compaction. Additionally, Iceberg connector version parity must be strictly verified rather than assumed.

## Decision
1. **Apache Flink `2.1.3` owns real-time stateful event-time streaming:**
   - Event-time timestamp extraction and bounded out-of-orderness watermarks (`30s` default allowance).
   - Online `event_id` deduplication with bounded keyed state (`StateTtlConfig`, 24h TTL).
   - Keyed vehicle state tracking and telemetry gap / offline vehicle detection using event-time timers.
   - Versioned deterministic + contextual anomaly rules and CEP patterns (e.g., `>= 3` hard-braking events within a 120s sliding window).
   - Late-event routing to side outputs (zero silent drops).
   - Outputs stream directly to Kafka (`fleet.anomalies.detected`, `fleet.vehicle-state`) and optionally to Iceberg via verified `org.apache.iceberg:iceberg-flink-runtime-2.1:1.12.0`.
2. **Durable Kafka -> Iceberg Bronze Ingestion Path:**
   - Even with `iceberg-flink-runtime-2.1:1.12.0` available, raw immutable Bronze ingestion is separated into a dedicated durable ingestion service (`services/ingestion`) so streaming anomaly logic changes or Flink savepoint upgrades **never interrupt raw evidence preservation**.
3. **Apache Spark `4.1.4` owns batch lakehouse curation & optimization:**
   - Bronze -> Silver canonical deduplication (handling duplicates beyond Flink's 24h state TTL), schema normalization, H3 spatial indexing, and geofence tagging.
   - Trip reconstruction and reconciliation across late arrivals.
   - SCD Type 2 `dim_vehicle` maintenance from PostgreSQL/Debezium CDC events.
   - Silver -> Gold operational KPIs, battery trends, software-version comparisons, and regional aggregations.
   - Idempotent historical backfills, small-file compaction (`rewrite_data_files`), snapshot expiration, and safe orphan-file cleanup.

## Consequences
- Clean failure isolation: Flink rule deployments cannot halt Bronze raw ingestion; Spark maintenance windows cannot delay real-time anomaly alerts.
- Late events beyond Flink's watermark allowance are still reconciled accurately into Silver and Gold on the next incremental Spark run.
