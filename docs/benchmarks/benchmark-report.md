# Benchmark Methodology & Measured Performance Report

All numbers reported here are 100% measured from reproducible scripts with raw JSON/CSV artifacts committed under `results/benchmarks/`. No unmeasured numbers are extrapolated.

## 1. Test Environment Specifications

- **Hardware:** Apple M4 Pro (`arm64`), 14 logical CPU cores, 48.0 GiB unified memory, 319 GiB free NVMe disk
- **OS & Runtime:** macOS Darwin `24.6.0`, Python `3.12.8`, `pyiceberg 0.12.0`, `duckdb 2.0.0`, `pyarrow 25.0.1`, `fastavro 1.13.1`, `h3 4.5.0`
- **Execution Profile:** `local_native` (`LocalWalEventBus` + `StatefulStreamEngine` + real Apache Iceberg Format V2 Parquet on local NVMe + DuckDB SQL)
- **Reproduction Commands:**
  - `make benchmark` -> `results/benchmarks/sustained_benchmarks.{json,csv}`
  - `make optimize-bench` -> `results/benchmarks/lakehouse_optimizations.json`

## 2. Sustained Workload Profile Results

| Profile | Virtual IDs | Active Vehicles | Ticks | Total Events | Sim (eps / MB/s) | Stream (eps) | Stream Batch p50 / p95 | Bronze Commit (eps) | Silver / Gold Curation | SQL p50 / p95 | Ckpt Save / Restore | Peak RSS |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`smoke`** | 100 | 100 | 12 | 1,742 | 32,322 / 11.82 | 28,319 | 38.7 / 38.7 ms | 15,179 | 0.08s / 0.08s | 9.66 / 10.60 ms | 1.20 / 0.58 ms | 274.9 MiB |
| **`local`** | 1,000 | 1,000 | 10 | 15,224 | 56,421 / 20.50 | 53,849 | 40.5 / 44.7 ms | 50,270 | 0.22s / 0.15s | 9.36 / 10.00 ms | 4.80 / 4.29 ms | 352.2 MiB |
| **`stress`** | 10,000 | 10,000 | 5 | 88,542 | 65,991 / 23.30 | 23,840 | 89.3 / 146.5 ms | 51,099 | 0.91s / 0.51s | 9.96 / 10.38 ms | 32.70 / 59.45 ms | 700.0 MiB |
| **`virtual-population`** | 1,000,000 | 2,000 | 8 | 25,069 | 59,505 / 21.53 | 42,819 | 46.2 / 58.6 ms | 52,034 | 0.32s / 0.22s | 9.54 / 10.30 ms | 8.30 / 7.37 ms | 723.6 MiB |

## 3. Storage Efficiency & Compression Ratios

- **Avro Binary vs. JSON:** `391 bytes` vs. `971 bytes` per canonical telemetry frame (**-59.73%**).
- **Iceberg ZSTD Parquet vs. Raw WAL:** Across `local`, `stress`, and `virtual-population`, Bronze Parquet consumed **0.134× to 0.140×** of WAL bytes (~7.2×–7.5× columnar compression efficiency).

## 4. Iceberg Lakehouse Optimization Summary (5-Day, 15,095-Row Table)

| Experiment | Baseline | Optimized | Speedup / Delta | Notes |
| :--- | :---: | :---: | :---: | :--- |
| **Partition Pruning (`days(event_time)`)** | 23.94 ms (20 files) | 6.23 ms (4 files) | **3.84× faster** | 80% of Parquet files skipped via Iceberg manifest stats |
| **Small-File Compaction** | 24.25 ms (20 files, 1.42 MB) | 5.47 ms (5 files, 1.16 MB) | **4.43× faster** | 18.5% smaller Parquet footprint; zero row loss |
| **Predicate Pushdown + Projection** | 13.37 ms | 2.23 ms | **6.00× faster** | Iceberg row filter + 3 projected columns |
| **Incremental 1-Day Backfill** | 379.83 ms | 373.96 ms | 1.02× (parity) | `pyiceberg` `overwrite()` rewrites untouched days; Spark dynamic partition overwrite avoids this |
| **Skew Salting (2-Stage)** | 0.81 ms | 1.33 ms | 0.61× (single-node overhead) | Exact numeric parity verified; speedup applies to distributed multi-executor shuffles |

## 5. Scaling Limitations

- Single-process `local_native` throughput is bounded by Python GIL serialization, full-table gap-timer scans at 10,000+ active keys, and full JSON checkpoint dumps.
- Multi-broker Kafka + multi-TaskManager Flink + distributed Spark/Trino deployments (`infra/kubernetes/fleet-platform.yaml`) are required to scale beyond single-node limits toward the 100k–1M active vehicle envelope modeled in `docs/capacity-model.md`.
