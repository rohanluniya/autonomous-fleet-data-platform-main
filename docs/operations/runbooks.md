# Operations Runbooks

Every Prometheus alert in `infra/observability/alerts.yml` maps directly to a concrete diagnosis and recovery procedure below.

## 1. DLQ Spike (`#dlq-spike`)
- **Alert:** `FleetDeadLetterQueueSpike` (`> 10` DLQ events in 5m)
- **Symptoms:** Elevated `fleet_dlq_messages_total` counter; rows appearing in `bronze.telemetry_dlq`.
- **Diagnosis:**
  1. Query recent DLQ errors via DuckDB/Trino: `SELECT error_code, error_message, COUNT(*) FROM bronze.telemetry_dlq GROUP BY 1, 2 ORDER BY 3 DESC`.
  2. Check recent producer or schema deployments against `tests/contracts/test_avro_contracts.py`.
- **Remediation:** Roll back offending producer firmware or register compatible Avro evolution in Apicurio Registry; replay valid affected time window after fix using `services/ingestion/replay.py`.

## 2. High Consumer Lag (`#consumer-lag`)
- **Alert:** `FleetConsumerLagHigh` (`> 50,000` records for 5m)
- **Symptoms:** Growing gap between topic high watermark and committed consumer offsets.
- **Diagnosis:**
  1. Inspect per-partition lag (`/platform/metrics` -> `consumer_lag_records`) to determine whether lag is uniform (under-provisioned workers) or concentrated on one partition (hot `vehicle_id` key).
  2. Check Flink backpressure and RustFS object storage write latency.
- **Remediation:** Scale `flink-taskmanager` replicas via Kubernetes HPA (`infra/kubernetes/fleet-platform.yaml`) or throttle rogue hot-key emitter.

## 3. Watermark Lag (`#watermark-lag`)
- **Alert:** `FleetStreamWatermarkLag` (`> 120s` for 3m)
- **Symptoms:** Delayed event-time window closure and telemetry-gap timers.
- **Diagnosis:** Check for idle Kafka partitions (vehicles offline in one partition holding global watermark back) or clock-skew anomalies.
- **Remediation:** Configure Flink idle source timeout (`with_idleness(Duration.of_seconds(60))`) and verify late-event side output volume.

## 4. Data Quality Failure (`#dq-failure`)
- **Alert:** `FleetDataQualityFailure` (`status="FAIL"` in `quality.dq_check_results`)
- **Symptoms:** Cross-layer reconciliation mismatch, uniqueness violation, or bounds breach.
- **Diagnosis:**
  1. Query `GET /fleet/data-quality` to isolate failing `check_name` and `details`.
  2. Inspect `silver.telemetry_quarantine` for GPS/kinematic bound violations.
- **Remediation:** If caused by a partial batch write or late-arriving backfill, execute an idempotent date-scoped backfill: `python pipelines/spark/local_batch_runner.py --backfill-date YYYY-MM-DD --run-dq`.

## 5. Gold Freshness Breach (`#freshness-breach`)
- **Alert:** `FleetGoldFreshnessBreach` (`> 1800s`)
- **Diagnosis:** Check Airflow `fleet_incremental_lakehouse` DAG task logs and Spark/Iceberg commit locks.
- **Remediation:** Re-trigger `fleet_incremental_lakehouse` DAG; Bronze retention in Kafka/WAL guarantees zero event loss during batch pauses.

## 6. High API Latency (`#api-latency`)
- **Alert:** `FleetApiP95LatencyHigh` (`p95 > 750ms`)
- **Diagnosis:** Query `GET /platform/metrics` -> `iceberg_tables` to inspect `data_file_count` on Gold/Silver tables (small-file explosion increases Parquet metadata scan overhead).
- **Remediation:** Run `python pipelines/spark/local_batch_runner.py --run-maintenance` (or trigger `fleet_iceberg_maintenance` DAG) to compact small Parquet files.

## 7. Standard Recovery Procedures

- **Stream Processor Checkpoint Restore:** Restart processor with `--restore` (`python services/stream-processing/runner.py --restore`) or in Flink: `flink run -s <savepoint_uri> ...`.
- **Idempotent Bounded Replay:** `python services/ingestion/replay.py --start-iso <UTC> --end-iso <UTC> --replay-batch-id <id>`, followed by Silver/Gold recomputation.
- **Iceberg Table Rollback:** Identify prior good `snapshot_id` via `table.snapshots()` (or Trino `$snapshots`) and roll back via `CALL fleet.system.rollback_to_snapshot('gold.fleet_activity_hourly', <snapshot_id>)`.
