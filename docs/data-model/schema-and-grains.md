# Medallion Data Model, Table Grains & Partitioning Specification

Every table in the lakehouse uses **Apache Iceberg Format V2** with **ZSTD Parquet** compression. Each table has an explicitly defined grain, business/primary key, and partition strategy avoiding high-cardinality anti-patterns (never partitioning directly by raw `vehicle_id` or minute-level timestamps).

## 1. Bronze Layer (Immutable Raw Evidence & Provenance)

| Table | Grain | Business / Deduplication Key | Partition Spec | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `bronze.telemetry_raw` | 1 row per ingested Kafka/WAL telemetry record (may include replays/duplicates) | `(event_id, ingest_time, source_offset)` | `days(event_time)` | Immutable raw sensor & GPS frame with `validation_status`, `replay_batch_id`, `source_partition`, `source_offset`. |
| `bronze.trips_raw` | 1 row per `trip_start` or `trip_end` event | `(event_id)` | `days(event_time)` | Raw trip lifecycle boundaries with odometer & battery start/end snapshots. |
| `bronze.autonomy_raw` | 1 row per synthetic autonomy disengagement event | `(event_id)` | `days(event_time)` | Explicitly flagged (`synthetic_flag=true`) disengagement records for trip risk analytics. |
| `bronze.anomalies_stream_raw` | 1 row per streaming anomaly emission | `(anomaly_id, detection_time)` | `days(event_time)` | Raw versioned anomalies emitted by Flink / stateful stream processor. |
| `bronze.telemetry_dlq` | 1 row per quarantined corrupt or schema-invalid payload | `(dlq_id)` | Unpartitioned (low volume) | Zero-silent-drop quarantine retaining base64 raw payload, `error_code`, and source offset. |

## 2. Silver Layer (Canonical Deduplicated, Enriched & SCD Type 2)

| Table | Grain | Primary / Natural Key | Partition Spec | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `silver.telemetry_clean` | 1 canonical validated telemetry sample per unique event | `event_id` | `days(event_time)`, `bucket(16, vehicle_id)` | Deduplicated (earliest `ingest_time` wins), H3 res-6/res-9 indexed, Shapely geofence-tagged, route-deviation measured, point-in-time joined with `vehicle_sk`. |
| `silver.trips_reconstructed` | 1 row per vehicle trip | `trip_id` | `days(start_time)` | Reconstructed trip distance (haversine + odometer cross-check), duration, speed stats, battery drain, anomaly counts, and synthetic disengagement count. |
| `silver.anomalies_verified` | 1 canonical anomaly per unique detection | `anomaly_id` | `days(event_time)` | Deduplicated explainable anomalies retaining `rule_id`, `rule_version`, `severity`, `evidence_json`, `source_event_ids_csv`. |
| `silver.dim_vehicle_scd2` | 1 row per `(vehicle_id, effective_from)` dimensional version | `vehicle_sk` (`VSK-sha256(vehicle_id\|effective_from)[:16]`) | Unpartitioned | SCD Type 2 tracking ONLY slow-changing attributes (`hardware_generation`, `vehicle_model`, `assigned_region`, `software_version`, `fleet_tier`, `maintenance_status`) with `effective_from`, `effective_to`, `is_current`, `cdc_lsn`. |
| `silver.telemetry_quarantine` | 1 row per DQ-rejected valid-Avro record | `event_id` | Unpartitioned | Quarantines out-of-bounds GPS, implausible speed/acceleration, or invalid battery SoC rows with diagnostic `quarantine_reason`. |

### Why SCD Type 2 Is Scoped Strictly to Dimensional Metadata
Applying SCD2 to fast-changing telemetry fields (speed, battery SoC, coordinates) would explode dimension row counts by 10,000x. Only slow-changing operational registry attributes arriving via PostgreSQL/Debezium CDC trigger new SCD2 versions, allowing historical anomaly rates to accurately reflect the `software_version` and `hardware_generation` active at each event's `event_time`.

## 3. Gold Layer (Curated Analytical & Serving Datasets)

| Table | Grain | Primary Key | Partition Spec | Primary Consumer |
| :--- | :--- | :--- | :--- | :--- |
| `gold.fleet_activity_hourly` | 1 row per hour, region, and software version | `(hour_bucket, region, software_version)` | `identity(event_date)` | `/fleet/summary`, Trino KPIs |
| `gold.anomaly_rates_by_segment` | 1 row per day, region, model, software version, rule, severity | `(event_date, region, vehicle_model, software_version, rule_id, severity)` | Unpartitioned | Software-version & model comparisons |
| `gold.trip_risk_summary` | 1 row per reconstructed trip | `trip_id` | Unpartitioned | `/fleet/trips`, risk leaderboard |
| `gold.battery_health_trends` | 1 row per day, region, vehicle model, software version | `(event_date, region, vehicle_model, software_version)` | Unpartitioned | Battery thermal & SoC degradation tracking |
| `gold.regional_h3_density` | 1 row per day, region, and H3 res-6 cell | `(event_date, region, h3_cell_res6)` | Unpartitioned | `/fleet/regions`, privacy-safe MapLibre heatmap |
| `gold.telemetry_reliability_kpis` | 1 row per day and region | `(event_date, region)` | Unpartitioned | Bronze-to-Silver dedup rate, late arrival rate, p95 lag |
| `gold.vehicle_latest_snapshot` | 1 row per active/registered vehicle | `vehicle_id` | Unpartitioned | `/fleet/vehicles`, operational fleet overview |

## 4. Governance & Quality Layer

| Table | Grain | Primary Key | Purpose |
| :--- | :--- | :--- | :--- |
| `quality.dq_check_results` | 1 row per check per DQ run | `(run_id, check_name)` | Historical pass/warn/fail quality audit backing `/fleet/data-quality` and alert rules. |
