# ADR-006: Dual Execution Profiles & Apple Silicon / Restricted-Host Compatibility

- **Status:** Accepted
- **Scope:** Local development portability across Linux x86_64, macOS Apple Silicon, and restricted corporate workstations

## Context
During Phase 0 host inspection on the development machine (Apple M4 Pro, macOS 15.x / Darwin 24.6 arm64, 14 cores, 48 GB RAM), we identified two real environmental constraints:
1. No container daemon (`docker`, `podman`, `colima`, or `orbstack`) is installed.
2. Endpoint binary allowlisting (`Santa`) blocks execution of newly downloaded third-party macOS JVM binaries (such as Eclipse Temurin `java`), preventing native JVM launch of Kafka, Flink, or Spark outside containers on this specific workstation.
3. Separately, upstream Docker images for `postgis/postgis` and `marquezproject/marquez:0.51.1` still lack full `linux/arm64` manifests.

## Decision
Implement a clean, modular **Dual-Profile Execution Architecture** with zero business-logic duplication and strict transparency:

1. **Containerized Distributed Profile (`docker` / `kubernetes`):**
   - Modular Docker Compose profiles (`core`, `analytics`, `observability`, `full`) in `infra/docker/docker-compose.yml`.
   - Native `arm64` + `amd64` images everywhere possible, including `imresamu/postgis:17-3.6.1-alpine3.23` for PostGIS.
   - Explicit `platform: linux/amd64` Rosetta 2 fallback for `marquezproject/marquez:0.51.1`.
   - Real PyFlink 2.1.3 jobs, PySpark 4.1.4 jobs, Trino 484 SQL, Debezium 3.7.1 CDC connectors, and Airflow 3.4.0 DAGs.
2. **Verified Native Zero-Docker Fallback Profile (`local_native`):**
   - Runs natively in Python 3.12 (`uv`) using real **Apache Iceberg Format V2** (`pyiceberg==0.12.0` + PyArrow Parquet + SQLite Iceberg catalog), real **DuckDB `2.0.0`** SQL over Iceberg, real **Avro** (`fastavro`), real **H3 `4.5.0`** + **Shapely** geospatial intelligence, real **FastAPI + MapLibre GL JS** serving, real **Prometheus + OpenTelemetry + OpenLineage JSONL**, and a deterministic local partitioned Write-Ahead Log (`local_wal`) + checkpointed stateful event-time stream engine sharing the exact same schema contracts and anomaly rule logic.
   - Every test report, benchmark artifact, API `/health` payload, and dashboard header explicitly surfaces `execution_profile: "local_native"` vs `"docker"` so reviewers always know the exact runtime engine behind every measured result.

## Consequences
- **Positive:** The entire platform can be executed, verified, fault-tested, benchmarked, and screenshotted deterministically on restricted laptops without Docker, while deploying unchanged business logic to full distributed Kafka/Flink/Spark/Trino clusters via Docker Compose or Kubernetes.
- **Honesty invariant:** Local fallback results are never reported as JVM Flink/Spark/Kafka cluster benchmarks; distributed cluster benchmarks are isolated in dedicated workflows and clearly labeled.
