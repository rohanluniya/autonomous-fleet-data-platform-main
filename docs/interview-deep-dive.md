# 5–10 Minute Demo Walkthrough & Staff-Level Interview Deep Dive

## Part A: Reproducible 5–10 Minute Live Demo Script

Run these commands from the repository root (`autonomous-fleet-data-platform/`):

1. **Minute 0–1 — Environment Detection & Architecture Overview**
   ```bash
   make detect-env
   ```
   Show automatic detection of host architecture (`arm64` / `x86_64`), memory budget, and transparent selection of `docker` vs. `local_native` profile (`ADR-006`).
2. **Minute 1–3 — End-to-End 1,000-Vehicle Pipeline Run**
   ```bash
   make run-demo
   ```
   Walk through live structured JSON logs showing:
   - `1,000` active synthetic vehicles emitting Avro events with controlled duplicates, out-of-order delayed flushes, 5 corrupt poison payloads, and a known 3x hard-braking CEP burst on `VEH-000001`.
   - Stateful stream processor suppressing duplicates, advancing bounded watermarks, emitting `RULE_REPEATED_HARD_BRAKING_CEP` (`CRITICAL`), and saving atomic checkpoints.
   - Durable Bronze Iceberg V2 ingestion quarantining all 5 poison messages to `bronze.telemetry_dlq` without halting valid ingestion.
   - CDC SCD Type 2 `dim_vehicle_scd2` reconstruction + Silver dedup/H3 enrichment + Gold KPI materialization + 11/11 Data Quality checks passing.
3. **Minute 3–5 — Interactive Dashboard, RBAC Privacy & Prometheus Metrics**
   ```bash
   make serve-api
   ```
   Open `http://127.0.0.1:8000`:
   - Highlight the `SIMULATED SYNTHETIC FLEET DATA` banner, `Event Time` vs. `Processing Time` strip, and self-hosted H3 res-6 GeoJSON map (zero external tile dependencies).
   - Demonstrate RBAC spatial coarsening: unauthenticated `GET /fleet/vehicles/VEH-000001` returns `coarsened_h3_res6`, while `Authorization: Bearer dev-admin-token-change-me` returns `raw_gps_authorized` and writes an `audit.raw_location_read` JSON log.
   - Show live Prometheus metrics at `http://127.0.0.1:8000/metrics`.
4. **Minute 5–8 — Controlled Fault Injection & Verification**
   ```bash
   make test-fault
   ```
   Show all 6 automated fault-injection tests passing in < 4 seconds (broker outage retry, stream TaskManager crash & checkpoint restore with exact anomaly parity, late-event side output, poison DLQ + DQ quarantine, double-replay Gold idempotence, hot-key skew salting).

---

## Part B: Senior / Staff Data Engineering Interview Discussion Guide

### 1. Partitioning & Ordering Guarantees
- **Question:** *If you partition Kafka by `vehicle_id`, are events globally ordered? Are events for one vehicle guaranteed to arrive in event-time order?*
- **Answer:** No to both. `murmur2(vehicle_id) % 12` guarantees per-key broker append order *within one partition*, never global ordering across partitions. Moreover, a single vehicle emerging from a cellular tunnel flushes buffered telemetry out of chronological order. Event-time correctness MUST be enforced via Flink watermarks (`30s` bounded out-of-orderness) plus late-event side outputs, never assumed from Kafka offsets.

### 2. Bounded Streaming State vs. 1M-Vehicle Scale
- **Question:** *Why does naive 24-hour `event_id` deduplication in Flink break at 1,000,000 concurrent vehicles?*
- **Answer:** As proved in `docs/capacity-model.md`, 1M vehicles at 1.15 Hz generate ~99.4 billion events/day (~4.8 TB of RocksDB dedup state!). Production mitigation combines short Flink TTL windows (15–60 min) or O(1)-per-vehicle monotonic `(vehicle_id, sequence_number)` tracking with partition-pruned Spark Iceberg `MERGE` dedup in Silver.

### 3. Why Not Claim "End-to-End Exactly-Once"?
- **Question:** *Your stack uses Kafka, Flink, Spark, and Iceberg — why didn't you advertise end-to-end exactly-once?*
- **Answer:** Because heterogeneous systems do not share a distributed two-phase commit coordinator. Instead, we architect **idempotent at-least-once boundaries**: deterministic UUIDv5 `event_id`s from producers, offset commits strictly *after* Iceberg Bronze commits, append-only immutable Bronze evidence, canonical `event_id` dedup in Silver, and atomic Iceberg snapshot overwrites in Gold. Even double-replaying Bronze yields identical Gold KPIs.

### 4. Lakehouse Consistency, Compaction & Safe Orphan Cleanup
- **Question:** *Why is orphan-file deletion dangerous if run aggressively?*
- **Answer:** Concurrent streaming or batch writers stage uncommitted Parquet files before committing their Iceberg manifest snapshot. Deleting unreferenced files younger than a strict safety window (`>= 72 hours`) would delete in-flight Parquet files and corrupt an upcoming snapshot commit.

### 5. CDC vs. Event-Time Telemetry & SCD Type 2
- **Question:** *Why do you run Debezium CDC separately from vehicle telemetry, and why not SCD2 all fields?*
- **Answer:** Operational metadata (firmware deployments, fleet tier, region reassignment, decommissioning) lives in transactional PostgreSQL and represents slow-changing dimensional truth, not high-frequency sensor streams. Tracking only 6 slow-changing attributes in `silver.dim_vehicle_scd2` enables accurate point-in-time joins (`event_time BETWEEN effective_from AND effective_to`) without dimension explosion.

### 6. Observability & OpenLineage Reality Check
- **Question:** *Does OpenLineage automatically capture end-to-end lineage for Flink, Spark, and Airflow?*
- **Answer:** Spark (`openlineage-spark_2.13:1.55.0`) and Airflow (`apache-airflow-providers-openlineage`) provide mature automatic dataset lineage to Marquez. Flink's OpenLineage integration remains narrower/evolving; rather than pretending automatic Flink lineage works identically everywhere, we explicitly document the gap and emit standardized OpenLineage v2 events on batch, ingestion, and CDC job boundaries.

### 7. Security & Privacy by Design
- **Question:** *How do you prevent an operational dashboard from leaking exact vehicle movement traces?*
- **Answer:** Default RBAC denies exact latitude/longitude to `viewer` and `analyst` roles, coarsening all points to H3 resolution 6 (`~36 km^2`) server-side in `apps/fleet-api/security.py`. Privileged `operator`/`admin` reads trigger structured JSON `audit.raw_location_read` logs, and map visuals use a self-hosted GeoJSON vector layer without third-party tile tracking.
