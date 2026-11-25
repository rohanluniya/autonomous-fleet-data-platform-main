# Incident Report INC-001: Controlled Fault-Injection Drill (Stream Crash, Broker Outage, Poison Surge & Double Replay)

- **Incident Type:** Planned Chaos & Reliability Verification Drill (Phase 12)
- **Execution Profile:** `local_native` (Apple M4 Pro, Python 3.12.8, Apache Iceberg Format V2)
- **Raw Evidence Artifact:** [results/fault_injection/fault_injection_results.json](../../results/fault_injection/fault_injection_results.json)
- **Overall Drill Verdict:** **6 / 6 Scenarios Recovered Without Data Loss or Duplicate Business Effects**

## 1. Executive Summary

To validate transactional boundaries, offset-after-commit ingestion, checkpoint recovery, dead-letter quarantine, and backfill idempotence, we executed six controlled failure scenarios via `pytest -v tests/fault/`. Every scenario met its recovery invariant with zero silent data drops and zero duplicate business counts in Gold tables.

## 2. Scenario Timeline & Measured Recovery Evidence

| # | Injected Failure | Expected Behavior | Measured Recovery Time | Observed Evidence | Outcome |
| :---: | :--- | :--- | :---: | :--- | :---: |
| **1** | **Broker Outage During Produce** (`set_broker_online(False)`) | Producer catches `BrokerUnavailableError`, backs off, retries until broker returns | `55.49 ms` | `2` retries before broker recovery; `1` record committed; `0` data loss | **PASS** |
| **2** | **Stream Processor Crash Mid-Stream** (`fail_after_records=245`, checkpoint every `100`) | New worker restores last periodic checkpoint, replays uncommitted offsets, matches uninterrupted baseline | `8.36 ms` | Baseline: `468` events / `10` anomalies; Recovered: `468` events / `10` anomalies; final lag `0` | **PASS** |
| **3** | **Out-of-Order Late Arrival (90s Behind Watermark) + Duplicate Surge** | Duplicate suppressed online; late event routed to side output (never dropped); Silver retains both unique events | `54.23 ms` | `1` duplicate suppressed; `1` late event routed to side output; `2/2` unique events in Silver | **PASS** |
| **4** | **Corrupt Binary Poison Messages (4x) + Implausible Speed (`180 m/s`)** | Binary corrupt payloads quarantined to `bronze.telemetry_dlq`; valid-Avro bad speed routed to `silver.telemetry_quarantine` | `85.85 ms` | `4` DLQ records; `1` Silver quarantine (`IMPLAUSIBLE_SPEED`); `0` silent drops | **PASS** |
| **5** | **Double Bounded Replay + Backfill + Duplicate CDC LSNs** | Immutable Bronze logs replay batches; Silver dedup & SCD2 LSN dedup keep Gold & `dim_vehicle_scd2` identical | `368.33 ms` | Gold events before: `120`; after double replay + backfill: `120`; SCD2 rows identical (`25`) | **PASS** |
| **6** | **Incompatible Schema + 80% Hot-Key Skew** | Incomplete Avro record rejected prior to send; 8-way salted aggregation matches direct mean across `201` keys | `0.48 ms` | `SchemaValidationError` raised; salted vs. direct numeric parity `True` | **PASS** |

## 3. Root-Cause Lessons & Operational Follow-Ups

1. **Replay-Safe Data Quality Thresholds:** During early drill runs, full-table Bronze replays triggered false-positive `duplicate_ingestion_rate_pct` DQ failures. We updated `DataQualityRunner` to compute live ingestion duplicate rates over `WHERE replay_batch_id IS NULL` while separately surfacing `replayed_bronze_rows` for operator auditability.
2. **Checkpoint Interval Sizing Tradeoff:** Frequent checkpoints (`every 100 events` in drill) minimized replay latency (`8.36 ms`) but increase I/O overhead at 10,000+ active keys (`59.45 ms` restore for `7.08 MB` JSON state). Production Flink clusters should run incremental RocksDB checkpoints every 15–30 seconds backed by object storage.
3. **Alert Coverage:** Verified alert mappings in `infra/observability/alerts.yml` (`FleetDeadLetterQueueSpike`, `FleetConsumerLagHigh`, `FleetDataQualityFailure`) link directly to remediation steps in `docs/operations/runbooks.md`.
