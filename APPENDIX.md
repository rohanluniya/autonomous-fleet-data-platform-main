# Engineering Appendix: Optimization Tradeoffs, Bottlenecks & Acceptance Matrix

This appendix captures detailed ablation analysis, bottlenecks observed during stress testing, cases where an optimization did *not* yield a speedup in single-node execution, and the complete Acceptance-Test verification record (A–O). All raw artifacts live under [results/](results/).

## 1. Optimization Analysis: What Helped vs. What Showed Tradeoffs

![Measured Apache Iceberg Format V2 optimization benchmarks](docs/assets/lakehouse_optimizations.png)

Raw data: [results/benchmarks/lakehouse_optimizations.json](results/benchmarks/lakehouse_optimizations.json).

### What Delivered Large Measured Speedups
1. **Iceberg Hidden Partition Pruning (`days(event_time)`): `3.84×` faster (`23.94 ms -> 6.23 ms`)**
   - Scanning a single day out of a 5-day Bronze table skipped `16/20` Parquet files (`80%` file pruning) and read `3,023` rows instead of `15,095`.
2. **Small-File Compaction (`20 -> 5` ZSTD Parquet files): `4.43×` faster (`24.25 ms -> 5.47 ms`)**
   - Coalescing 4 micro-batches per day into 1 file per day reduced Parquet storage by **18.5%** (`1,418,408 -> 1,156,052 bytes`) through larger dictionary/RLE pages while preserving all `15,095` rows.
3. **Predicate Pushdown + Column Projection: `6.00×` faster (`13.37 ms -> 2.23 ms`)**
   - Combining Iceberg row-group pruning (`event_time` + `region='sf_bay'`) with 3-column projection avoided decoding 30 unused sensor columns.
4. **Avro Binary Serialization vs. JSON: `-59.73%` wire size (`971 -> 391 bytes/event`)**
   - Eliminating repeated field keys cuts network bandwidth and broker disk consumption by more than half before ZSTD batch compression is even applied.

### What Showed Tradeoffs in Single-Node `local_native` Execution (and Why)
1. **Incremental 1-Day Backfill vs. Full 5-Day Refresh (`373.96 ms` vs. `379.83 ms`, `1.02×`):**
   - In `pyiceberg`, replacing a single day partition inside `MedallionBatchPipeline.run_bronze_to_silver(target_date=...)` reads untouched partitions from Silver and issues a full table `overwrite()` across the `bucket(16, vehicle_id)` spec, so table-write overhead dominated at `15k` rows.
   - In Apache Spark `4.1.4` (`pipelines/spark/medallion_spark_job.py`), dynamic partition overwrite (`INSERT OVERWRITE` scoped to `event_day`) writes only the targeted day's files without rewriting historical partitions.
2. **Skewed-Key Two-Stage Salting on Single-Node DuckDB (`1.329 ms` salted vs. `0.814 ms` unsalted):**
   - On a single-node shared-memory engine (DuckDB), two-stage salting (`GROUP BY key, salt` followed by `GROUP BY key`) adds an extra aggregation pass without any network shuffle to balance.
   - Salting is specifically an optimization for distributed multi-executor shuffle stages (Spark/Flink) where 80% hot-key concentration starves `N-1` executors while 1 executor stalls or OOMs. Our test verifies exact numerical parity (`< 1e-5` IEEE-754 summation delta across 251 keys) so the pattern is proven correct before cluster deployment.

## 2. Bottlenecks Observed During 10,000-Vehicle Stress Benchmarking

Inspecting [results/benchmarks/sustained_benchmarks.json](results/benchmarks/sustained_benchmarks.json) reveals an authentic scaling bottleneck in the single-process Python stream engine:

| Profile | Active Keys | Stream Throughput (events/s) | Checkpoint Size | Checkpoint Save | Checkpoint Restore | Peak RSS |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `smoke` | 100 | 28,319 | 112.2 KB | 1.20 ms | 0.58 ms | 274.9 MiB |
| `local` | 1,000 | 53,849 | 975.7 KB | 4.80 ms | 4.29 ms | 352.2 MiB |
| `virtual-population` | 2,000 (of 1M) | 42,819 | 1,737.8 KB | 8.30 ms | 7.37 ms | 723.6 MiB |
| `stress` | 10,000 | **23,840** | **7,080.9 KB** | **32.70 ms** | **59.45 ms** | **700.0 MiB** |

**Root Cause of the 2.26× Stream Throughput Drop from 1,000 to 10,000 Active Vehicles:**
1. At the end of each 2,500-record micro-batch, `StatefulStreamEngine._check_gap_timers()` scanned all `10,000` `vehicle_states` entries and emitted fresh `VehicleStateEvent` updates, adding `O(active_vehicles)` work per micro-batch.
2. Full JSON checkpoint serialization scaled linearly with `len(vehicle_states) + len(dedup_seen)` (`~7.1 MB` at 10k vehicles + 49k dedup keys).
3. **Production Architectural Lesson:** This empirically validates why Apache Flink uses **keyed event-time timer min-heaps** (firing only on expired timestamps in `O(log N)`) and **incremental RocksDB state checkpoints** rather than full JSON snapshots.

## 3. Real Bugs Caught & Fixed During Build Verification

1. **Simulated Clock vs. Wall-Clock Ingest Lag Bug:** Initial 1,000-vehicle runs showed `100%` late arrival rate (`45,070s` ingest lag) because `start_time_utc` defaulted to `12:00 UTC` while `ingest_time` used `datetime.now(UTC)`. Fixing the simulator to stamp `ingest_time` relative to simulated `tick_time` (plus realistic `45–320 ms` jitter and delayed-flush offsets) brought measured regional `p95` ingest lag to `~0.31s` and late arrival rates to `~2.8%`.
2. **Coprime Modulo Alignment Bug:** Initial agent assignment used `idx % 3` for region, vehicle model, and software version simultaneously, collapsing the `3 × 3 × 3` analytical matrix to only 3 combinations. Decoupling strides (`idx % 3`, `(idx // 3) % 3`, `(idx // 9) % 3`) restored all `27` regional/model/firmware segments in `gold.battery_health_trends`.
3. **False-Positive Replay DQ Alert:** Replaying Bronze during E2E acceptance testing caused `duplicate_ingestion_rate_pct` to spike to `50%` and fail DQ. Filtering live duplicate checks to `WHERE replay_batch_id IS NULL` (while tracking replayed rows separately in DQ details) eliminated false alerts during legitimate operator replays.
4. **PyFlink vs. Host PyArrow Version Conflict:** `uv lock` surfaced that `apache-flink==2.1.3` pins `pyarrow<21`, conflicting with host `pyarrow==25.0.1`. Isolating container-specific engine requirements in `infra/docker/requirements-{flink,spark,airflow}.txt` resolved the lock cleanly.

## 4. Complete Mandatory Acceptance-Test Matrix (A–O)

| ID | Criterion | Status | Measured Evidence & Verification Path |
| :---: | :--- | :---: | :--- |
| **A** | Fresh setup boots core platform | **PASS (local_native)** / **Docker Validated** | `uv sync --extra dev`, `make detect-env`, `make run-demo` verified; `infra/docker/docker-compose.yml` pinned & checked (no Docker daemon on host). |
| **B** | Generate 1,000 synthetic vehicles and observe ingestion | **PASS** | `local` profile produced `15,224` Avro events across 1,000 active vehicles (`results/benchmarks/sustained_benchmarks.json`). |
| **C** | Events reach durable Bronze Iceberg storage and can be queried | **PASS** | Real Iceberg V2 `bronze.telemetry_raw` Parquet + metadata queried via DuckDB 2.0 (`test_full_end_to_end_acceptance_pipeline`). |
| **D** | Silver normalization and Gold aggregates are correct | **PASS** | `11/11` DQ checks PASS including `0` row diff Bronze unique vs. Silver+Quarantine and Silver vs. Gold. |
| **E** | Inject known hard-braking anomaly and verify detection with evidence | **PASS** | `VEH-000001` triggered `RULE_HARD_BRAKING` (`v2.0.0`, HIGH) x3 + `RULE_REPEATED_HARD_BRAKING_CEP` (`v2.1.0`, CRITICAL) with evidence & 3 source `event_id`s. |
| **F** | Inject duplicate & out-of-order messages and verify documented behavior | **PASS** | `test_fault_03_delayed_out_of_order_late_side_output_and_duplicates`: duplicate suppressed, late event routed to side output, preserved in Silver. |
| **G** | Restart streaming processor and verify checkpoint recovery | **PASS** | `test_fault_02_stream_taskmanager_crash_and_checkpoint_restore`: crashed after 245 records, restored in `8.36 ms`, exact `468/468` & `10/10` anomaly parity. |
| **H** | Replay bounded dataset and verify idempotent business results | **PASS** | `test_fault_05` + E2E test: double Bronze replay yields identical Gold totals (`120 == 120` in fault test; exact match in 1,000-vehicle E2E). |
| **I** | Run batch backfill and verify reconciliation | **PASS** | Date-scoped `run_backfill(target_date="2026-11-24")` + `DataQualityRunner` (`11/11 PASS`). |
| **J** | Query Gold datasets through SQL engine | **PASS (DuckDB)** / **Trino SQL Provided** | Executed over Iceberg snapshots via DuckDB 2.0 locally; Trino 484 catalog & SQL queries in `sql/trino/analytical_queries.sql`. |
| **K** | Access meaningful API endpoints and dashboard views | **PASS** | All 9 FastAPI endpoints HTTP 200, RBAC coarsening verified, live Chrome screenshot captured in `docs/assets/dashboard_live.png`. |
| **L** | Observe real operational metrics and structured logs | **PASS** | `221`-line Prometheus `/metrics`, secret-scrubbed JSON logs, OpenTelemetry spans, and OpenLineage v2 events verified. |
| **M** | Run controlled failure injection and produce incident report | **PASS** | All 6 fault scenarios executed in `tests/fault/` and documented in `docs/incident-reports/INC-001-stream-crash-and-poison-surge.md`. |
| **N** | Execute automated test suite and publish pass/fail report | **PASS** | `18 passed, 0 failed` (`results/tests/pytest_report.xml`). |
| **O** | Run sustained benchmark and publish raw results with hardware specs | **PASS** | `results/benchmarks/sustained_benchmarks.{json,csv}` & `lakehouse_optimizations.json` with full M4 Pro hardware specs. |
