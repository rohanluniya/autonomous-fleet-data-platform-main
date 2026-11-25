# Known Limitations, Execution Boundaries & Honest Tradeoffs

This repository prioritizes verifiable engineering truth over marketing polish. Every current limitation and execution boundary is documented explicitly below.

## 1. Host Verification Environment vs. Full Containerized Cluster
- **Verified directly on this workstation (macOS Apple M4 Pro, 14 cores, 48 GiB RAM):**
  - `docker`, `podman`, `colima`, and a system JDK were absent on the build workstation, and corporate endpoint policy (`Santa`) blocks execution of newly downloaded unsigned/unallowlisted macOS JVM binaries.
  - Therefore, all live executions, automated tests, fault-injection proofs, screenshots, and sustained benchmarks recorded in `results/` ran under the explicitly labeled **`local_native`** execution profile: real **Apache Iceberg Format V2** (`pyiceberg==0.12.0`, real `metadata.json`, Avro manifests, ZSTD Parquet files), real **DuckDB `2.0.0`** SQL over Iceberg snapshots, real **Avro** (`fastavro`), real **H3 `4.5.0`** + **Shapely**, real **FastAPI + MapLibre GL JS**, real **Prometheus + OpenTelemetry + OpenLineage JSONL**, and our local partitioned WAL (`local_wal`) + checkpointed stateful event-time stream engine.
- **Not executed directly on this Docker-less host (validated statically & wired for Docker/CI runners):**
  - Multi-container JVM startup of `apache/kafka:4.3.2`, `flink:2.1.3`, `apache/spark:4.1.4`, `trinodb/trino:484`, `quay.io/lakekeeper/catalog:v0.15.1`, `rustfs/rustfs:1.0.6`, `quay.io/debezium/connect:3.7.1.Final`, `apache/airflow:slim-3.4.0`, and `marquezproject/marquez:0.51.1`.
  - Every Docker image tag, multi-arch (`amd64`/`arm64`) manifest, and Maven connector coordinate was verified live against Docker Hub, Quay.io, and Maven Central (`docs/compatibility.md`), and the scheduled GitHub Actions workflow runs containerized builds on Linux runners.

## 2. Scale Claims vs. Measured Envelopes
- **Measured locally:** Up to `10,000` concurrently active vehicles (`stress` profile) and `1,000,000` virtual vehicle identities with `2,000` concurrently active vehicles (`virtual-population` profile).
- **Analytical only (not locally executed):** `100,000+` and `1,000,000` concurrently active emitting vehicles are modeled analytically in `docs/capacity-model.md`, which explicitly flags the ~4.8 TB RocksDB 24-hour `event_id` dedup state bottleneck at 1.15M events/sec.

## 3. OpenLineage Integration Scope
- Spark 4.1.4 (`openlineage-spark_2.13:1.55.0`), Airflow 3.4.0, and our local Python Bronze/CDC/Medallion runners emit valid OpenLineage v2 `START`/`COMPLETE`/`FAIL` events.
- Native automatic OpenLineage emission inside PyFlink DataStream jobs is not assumed or fabricated; Flink dataset lineage is documented via job-level contract metadata.

## 4. Development Security vs. Production Hardening
- Local development uses static `.env` bearer tokens (`FLEET_API_*_TOKEN`) and plaintext internal container links inside `fleet_internal`.
- Production rollout requires OIDC/JWKS identity verification, mTLS across Kafka/Flink/Spark/Trino/Lakekeeper, external secrets management (Vault/KMS), and encrypted object storage (`docs/threat-model.md`).

## 5. Synthetic Telemetry Disclaimer
- All vehicle IDs, kinematics, sensor degradations, and autonomy disengagement events are synthetically generated from deterministic mathematical models. Nothing in this platform represents real-world autonomous vehicle performance or safety certification data.
