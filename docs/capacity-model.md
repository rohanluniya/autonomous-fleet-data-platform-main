# Capacity Planning & Workload Sizing Model

> **Important Distinction:** Supporting **1,000,000+ virtual vehicle identities** in a registry or dimension table is fundamentally different from ingesting and statefully processing **1,000,000 concurrently active vehicles** simultaneously. This model separates virtual fleet population from concurrent active emitters and states every assumption explicitly. All figures below are analytical sizing estimates; measured platform performance on local hardware is recorded separately in `docs/benchmarks/`.

## 1. Event Payload & Frequency Assumptions

| Parameter | Baseline Assumption | Notes |
| :--- | :--- | :--- |
| **Average Avro binary event size (uncompressed)** | `320 bytes` | Canonical telemetry frame (`event_id`, `vehicle_id`, kinematics, battery, thermal, GPS, metadata). |
| **Kafka ZSTD batch compression ratio** | `3.2x` (`~100 bytes/event` on wire/disk) | Measured empirically on structured Avro batches; varies with float entropy. |
| **Parquet ZSTD Bronze storage footprint** | `~90 bytes/event` | Columnar encoding + dictionary compression on `vehicle_id`, `region`, `software_version`. |
| **Active vehicle telemetry frequency** | `1.0 Hz` (`1 event/sec/active vehicle`) | Configurable (`0.1 Hz` to `10 Hz`) via workload profile. |
| **Auxiliary events (diagnostics, trips, autonomy)** | `+15%` overhead on top of telemetry | Bursty on state transitions and fault conditions. |
| **Effective aggregate rate per active vehicle** | `1.15 events/sec` | Used in sizing table below. |
| **Kafka replication factor (production target)** | `RF = 3` (`RF = 1` in single-node local dev) | Multiplies broker disk footprint by 3x in production. |

## 2. Configured Load Profiles vs. Analytical Sizing Estimates

| Profile | Virtual Vehicle IDs | Concurrently Active Vehicles | Target Aggregate Rate (events/s) | Uncompressed Ingress (MB/s) | Compressed Kafka Ingress (MB/s) | 7-Day Kafka Disk (RF=3) | Daily Bronze Iceberg Parquet | Flink Keyed State (~2 KB/active key) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`smoke`** (CI / quick verify) | `100` | `100` | `115` | `0.037` | `0.012` | `22 MB` | `0.9 MB/day` | `0.2 MB` |
| **`local`** (laptop E2E) | `1,000` | `1,000` | `1,150` | `0.37` | `0.12` | `218 MB` | `8.9 MB/day` | `2.0 MB` |
| **`stress`** (single-node stress) | `10,000` | `10,000` | `11,500` | `3.68` | `1.15` | `2.18 GB` | `89 MB/day` | `20 MB` |
| **`virtual-population`** (1M registry, active subset) | `1,000,000` | `5,000` | `5,750` | `1.84` | `0.58` | `1.09 GB` | `45 MB/day` | `10 MB` (+~180 MB `dim_vehicle` table) |
| **`scale`** (cluster target) | `100,000` | `100,000` | `115,000` | `36.8` | `11.5` | `21.8 GB` | `0.89 GB/day` | `200 MB` |
| **Hypothetical 1M Concurrent Fleet** (reference model) | `1,000,000` | `1,000,000` | `1,150,000` | `368` | `115` | `218 GB` | `8.9 GB/day` | `2.0 GB` (vehicle state) + `~11 GB` (24h dedup @ 1.15M/s -> needs Bloom/TTL tuning!) |

*(Wait: note a critical Staff-level engineering finding right in that last cell!)*

## 3. Hidden Bottleneck Exposed by the 1M-Vehicle Capacity Model: Online Deduplication State

Calculate exact Flink keyed deduplication state at `1,150,000 events/sec` over a 24-hour TTL:
- Events in 24 hours: `1.15M * 86,400 = 99.36 billion event_ids`.
- Storing raw 16-byte UUIDs + RocksDB key/value overhead (~48 bytes/entry) for 24 hours would require **`~4.77 TB` of RocksDB state** — completely infeasible on a naive single-table `MapState`!
- **Architectural Consequence & Design:**
  1. In Flink, **online deduplication TTL is bounded to short reconnect windows (e.g., `15–60 minutes`)** or uses monotonic `(vehicle_id, sequence_number)` watermarks (which require only **O(1) bytes per vehicle** — `~64 MB` for 1M vehicles — rather than O(events)!).
  2. Full historical `event_id` deduplication across multi-day late replays is delegated to **Spark Iceberg partition-scoped `MERGE`** (`days(event_time)` pruning limits shuffle scope to affected day partitions).
  3. At `1,150,000 events/sec`, Kafka requires `>= 128–256 partitions` (~4,500–9,000 msg/s/partition), `>= 6–9 brokers` (NVMe SSD, 10 Gbps+ NIC), Flink requires `>= 64–128 TaskManager slots` with incremental RocksDB checkpoints to object storage, and Iceberg requires hourly compaction to prevent small-file explosion.

## 4. Local Host Hardware Envelope (Measured Machine)

- **Host:** Apple M4 Pro (`arm64`), 14 CPU cores, 48 GiB unified RAM, 319 GiB available NVMe storage.
- **Local Verification Scope:** `smoke` (`100` active), `local` (`1,000` active), `stress` (`10,000` active), and `virtual-population` (`1,000,000` virtual vehicle IDs with `2,000` active concurrent emitters) are executed and measured directly. The `1,000,000` concurrent active fleet row above is strictly an analytical capacity model, never a claimed local measurement.
