# Platform Requirements Specification

## 1. Business Questions & Functional Requirements

Every dataset, stream job, SQL view, API endpoint, and dashboard panel must directly answer one or more core operational and analytical questions for a synthetic autonomous mobility fleet:

| ID | Business Question | Primary Serving Path | Verification Target |
| :--- | :--- | :--- | :--- |
| **FR-1** | Which vehicles are active, offline, parked, charging, or experiencing telemetry gaps? | Flink keyed state + timers -> `fleet.vehicle-state` -> Gold `gold_fleet_activity` -> `/fleet/summary`, `/fleet/vehicles` | Gap detected when vehicle exceeds configurable silence threshold (`60s` default) |
| **FR-2** | Where are active vehicles located (with privacy-safe spatial aggregation by default)? | H3 cell indexing (`res=6` public / `res=9` operator) + PostGIS geofences -> `/fleet/regions` -> MapLibre dashboard | Public/analyst roles receive coarsened H3 hexagons; exact coordinates restricted to admin/operator roles |
| **FR-3** | Which vehicles exhibit unusual braking, acceleration, thermal, battery, or sensor health behavior? | Flink versioned rules + contextual baselines -> `fleet.anomalies.detected` -> `/fleet/anomalies` | Ground-truth injected anomalies matched with `rule_id`, `rule_version`, `evidence`, and source `event_id` |
| **FR-4** | Which trips contain repeated safety-relevant anomalies? | Flink sliding-window CEP (`>=3` hard brakes in `120s`) + Spark trip reconstruction -> Gold `gold_trip_risk_summary` -> `/fleet/trips` | Trip risk scores reconciled across streaming detections and batch trip windows |
| **FR-5** | How do anomaly rates vary by region, vehicle model, software version, and time? | Iceberg Gold `gold_software_version_kpis` + `gold_regional_kpis` (joined with SCD2 `dim_vehicle`) -> Trino / DuckDB -> `/fleet/regions`, `/fleet/summary` | Historical software version joins reflect version active *at `event_time`*, not current version |
| **FR-6** | How do late arrivals, duplicates, corrections, and schema changes affect analytics? | Flink bounded watermarks + late-event side output + Silver dedup + Avro schema evolution + Bronze quarantine | Automated contract, duplicate, out-of-order, and late-event reconciliation tests |
| **FR-7** | Can operators replay data and recover from processing failures without corrupting business results? | Bounded replay utility + Flink checkpoints + Iceberg atomic overwrites + idempotent Spark backfills | Fault-injection suite proves zero duplicate business counts after replay or restart |
| **FR-8** | What are freshness, reliability, throughput, and operational costs of the platform? | Prometheus metrics, structured JSON logs, OTel traces, DQ check store, and sustained benchmark suite -> `/fleet/data-quality`, `/platform/metrics` | Real measured throughput, latency percentiles, freshness lag, and resource utilization |

## 2. Non-Functional Requirements

- **Open Source Only:** Every runtime, library, schema format, catalog, and visualization tool must use an OSI-approved open-source license with zero paid cloud dependencies.
- **Deterministic Synthetic Data:** All fleet identities and telemetry are synthetic and seeded. Synthetic autonomy disengagement events are explicitly labeled as simulated and never presented as real-world driving safety evidence.
- **Schema Governance:** All event contracts are defined in Apache Avro (`schemas/avro/`) with automated backward/forward compatibility checks in CI.
- **Observability as First-Class:** Structured JSON logs (`timestamp`, `service`, `environment`, `severity`, `trace_id`, `correlation_id`, `pipeline_run_id`, `component`, `event_type`, `error_code`, `retry_count`), Prometheus metrics, OpenTelemetry spans, and OpenLineage dataset lineage.
- **Data Quality & Zero Silent Drops:** Invalid records, schema rejections, and GPS/kinematic bound violations are quarantined to `fleet.telemetry.dlq` or Iceberg quarantine tables with diagnostic reason codes rather than dropped silently.
- **Reproducibility:** Every workflow (environment check, tests, simulation, end-to-end pipeline, fault injection, benchmarks, dashboard launch, Docker Compose startup) is exposed via `Makefile` targets and scripts.
