# Compatibility Matrix, Pinned Releases & Licensing

Every container image, Maven artifact, and Python dependency in this repository is pinned to an explicit verified release. Floating tags such as `latest` are prohibited across Docker Compose, Kubernetes manifests, CI workflows, and lockfiles.

## 1. Container Stack (Docker Compose & Kubernetes)

All image tags, multi-architecture manifests, and open-source licenses were verified directly against Docker Hub, Quay.io, Maven Central, and upstream documentation before pinning.

| Component | Pinned Image / Artifact | License | `linux/amd64` | `linux/arm64` (Apple Silicon) | Role & Notes |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **Event Streaming** | `apache/kafka:4.3.2` | Apache-2.0 | Yes | Yes | KRaft consensus mode (ZooKeeper removed in Kafka 4.x); requires JDK 17+ inside image. |
| **Schema Registry** | `apicurio/apicurio-registry:3.3.5` | Apache-2.0 | Yes | Yes | OSI-approved Apache-2.0 alternative to source-available Confluent Schema Registry; exposes Confluent-compatible `/apis/ccompat/v7` and native v3 APIs. |
| **Stream Processing** | `flink:2.1.3-scala_2.12-java17` | Apache-2.0 | Yes | Yes | Stateful event-time processing, watermarks, RocksDB checkpoints, CEP anomaly rules. Aligned with Iceberg Flink runtime `2.1` and Kafka connector `5.0.0-2.1`. |
| **Batch & Lakehouse Compute** | `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu` | Apache-2.0 | Yes | Yes | Bronze-to-Silver-to-Gold batch pipelines, SCD Type 2 maintenance, backfills, compaction, and Spark optimization benchmarks. Aligned with Iceberg Spark runtime `4.1_2.13` and OpenLineage `1.55.0`. |
| **S3-Compatible Object Store** | `rustfs/rustfs:1.0.6` | Apache-2.0 | Yes | Yes | Drop-in S3-compatible object store (ports 9000/9001). Selected instead of MinIO CE, which halted public Docker Hub builds in Oct 2025 and archived on Apr 25, 2026 (see `ADR-002`). |
| **Iceberg REST Catalog** | `quay.io/lakekeeper/catalog:v0.15.1` | Apache-2.0 | Yes | Yes | Single authoritative Iceberg REST catalog written in Rust, backed by PostgreSQL; native interoperability with Spark, Flink, Trino, and PyIceberg. |
| **Analytical SQL Engine** | `trinodb/trino:484` | Apache-2.0 | Yes | Yes | Distributed SQL query layer over Lakekeeper Iceberg REST catalog (`fs.native-s3.enabled=true`). Image bundles required Java 25 runtime. |
| **Operational DB + Spatial Serving** | `imresamu/postgis:17-3.6.1-alpine3.23` | PostgreSQL / GPL-2.0 | Yes | Yes | PostgreSQL 17 + PostGIS 3.6 with `wal_level=logical` for CDC and spatial serving. Used as documented native ARM64+AMD64 build because upstream `postgis/postgis` lags on `arm64` manifests. |
| **CDC Connector** | `quay.io/debezium/connect:3.7.1.Final` | Apache-2.0 | Yes | Yes | Kafka Connect + Debezium PostgreSQL logical replication connector for vehicle registry reference data. |
| **Orchestration** | `apache/airflow:slim-3.4.0-python3.12` | Apache-2.0 | Yes | Yes | Airflow 3.x batch orchestration, backfill DAGs, DQ checks, reconciliation, and Iceberg table maintenance (Airflow 2 reached EOL in Apr 2026). |
| **Metrics Store** | `prom/prometheus:v3.16.0` | Apache-2.0 | Yes | Yes | Scrapes FastAPI, simulator, stream processor, Kafka/JMX, Flink, and OpenTelemetry collector metrics. |
| **Dashboards** | `grafana/grafana:13.0.14` | AGPL-3.0 | Yes | Yes | Provisioned operational dashboards for pipeline lag, checkpoints, freshness, DQ, and API latency. |
| **Telemetry Collector** | `otel/opentelemetry-collector-contrib:0.166.0-arm64` / `0.166.0-amd64` | Apache-2.0 | Yes | Yes | Receives OTLP HTTP/gRPC traces and metrics; platform suffix parameterized via `${OTEL_ARCH}` in Compose. |
| **OpenLineage Backend** | `marquezproject/marquez:0.51.1` | Apache-2.0 | Yes | Emulated (`linux/amd64`) | Upstream publishes `amd64`-only images. On Apple Silicon Docker, Compose sets `platform: linux/amd64` (Rosetta 2). Native zero-Docker mode uses the built-in local OpenLineage JSONL collector fallback. |

## 2. Verified Maven Connector Coordinates (Cross-Engine Iceberg Interoperability)

All connectors are aligned to **Apache Iceberg `1.12.0`** (Apache-2.0) and verified on Maven Central (`repo1.maven.org`):

| Engine | Maven Coordinate | Verified Version | Compatibility Rationale |
| :--- | :--- | :--- | :--- |
| **Spark 4.1.4 (Scala 2.13)** | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13` | `1.12.0` | Iceberg 1.12.0 supports Spark up to 4.1 (Spark 4.3 is not yet supported by Iceberg runtime JARs). |
| **Spark S3 FileIO** | `org.apache.iceberg:iceberg-aws-bundle` | `1.12.0` | S3FileIO implementation for RustFS / S3-compatible object storage. |
| **Spark OpenLineage** | `io.openlineage:openlineage-spark_2.13` | `1.55.0` | OpenLineage 1.34.x predates Spark 4.x logical plan changes; 1.42+ / 1.55.0 supports Spark 4.0/4.1 + Scala 2.13. |
| **Flink 2.1.3** | `org.apache.iceberg:iceberg-flink-runtime-2.1` | `1.12.0` | Iceberg 1.12.0 publishes runtime artifacts for Flink 1.20, 2.0, and 2.1 (not yet 2.3). |
| **Flink Kafka Connector** | `org.apache.flink:flink-sql-connector-kafka` | `5.0.0-2.1` | Exact minor-version match for Flink `2.1.3` and Kafka 4.x protocol. |

## 3. Core Python Dependencies (Locked via `uv.lock`)

Target Python runtime: **Python 3.12** (`>=3.12,<3.13`), chosen because `apache-flink==2.1.3`, `apache-airflow==3.4.0`, `pyiceberg==0.12.0`, `duckdb==2.0.0`, and `pyarrow==25.0.1` all publish compatible macOS `arm64` and Linux `x86_64` wheels for CPython 3.12.

| Package | Pinned Version | License | Purpose |
| :--- | :--- | :--- | :--- |
| `pyiceberg[sql-sqlite]` | `0.12.0` | Apache-2.0 | Genuine Apache Iceberg Format V2 catalog, table creation, appends, overwrites, schema evolution, snapshots, time travel, and compaction. |
| `duckdb` | `2.0.0` | MIT | High-performance local SQL query engine over Iceberg/Parquet tables and local fallback for Trino when Docker is unavailable. |
| `pyarrow` | `25.0.1` | Apache-2.0 | Zero-copy Arrow columnar tables and ZSTD/Snappy Parquet serialization. |
| `fastavro` | `1.13.1` | MIT | Binary Apache Avro serialization, deserialization, and schema compatibility checks. |
| `confluent-kafka` | `2.16.1` | Apache-2.0 | Production librdkafka-backed Kafka producer/consumer client for Kafka 4.x brokers. |
| `h3` | `4.5.0` | Apache-2.0 | Uber H3 hexagonal hierarchical spatial indexing. |
| `shapely` | `2.2.0` | BSD-3-Clause | Geofence polygon containment and route corridor deviation geometry checks. |
| `fastapi` / `uvicorn` | `0.163.2` / `0.54.0` | MIT / BSD-3-Clause | Typed serving API, RBAC enforcement, pagination, Prometheus `/metrics`, and dashboard hosting. |
| `prometheus-client` | `0.26.0` | Apache-2.0 | Native Prometheus metric instrumentation across simulator, stream engine, batch runner, and API. |
| `opentelemetry-sdk` | `1.45.2` | Apache-2.0 | Distributed trace context (`trace_id`, `span_id`) and OTLP export. |
| `pydantic` | `2.14.3` | MIT | Strict typed event contracts, configuration validation, and API response schemas. |

## 4. Platform Detection & Host-Specific Fallback Policy

The platform detects available runtime capabilities via `scripts/detect_env.py`:

1. **Docker + Multi-Arch Host (Linux x86_64 or macOS Apple Silicon with >= 12 GB Docker RAM):**
   - Runs full containerized profiles (`core`, `analytics`, `observability`, `full`) via `infra/docker/docker-compose.yml`.
   - Uses `apache/kafka:4.3.2`, `flink:2.1.3-scala_2.12-java17`, `apache/spark:4.1.4`, `quay.io/lakekeeper/catalog:v0.15.1`, `rustfs/rustfs:1.0.6`, `trinodb/trino:484`, `imresamu/postgis:17-3.6.1-alpine3.23`, `quay.io/debezium/connect:3.7.1.Final`, `apache/airflow:slim-3.4.0-python3.12`, `prom/prometheus:v3.16.0`, `grafana/grafana:13.0.14`, and `marquezproject/marquez:0.51.1`.
2. **Restricted Local Workstation Without Docker or JVM (Verified on Host macOS Apple M4 Pro, 48 GB RAM, Santa binary allowlisting):**
   - When `docker` is absent and third-party macOS JVM binaries are blocked by endpoint policy (`Santa`), the platform activates its explicitly labeled **`local_native`** execution profile:
     - **Event Transport:** Partitioned append-only local Write-Ahead Log (`local_wal`, Murmur2 key partitioning by `vehicle_id`, consumer-group offset commits, DLQ topics, time-range replay) with identical Avro schemas and topic topology to Kafka (`kafka` adapter remains production code).
     - **Stream Processing:** Pure-Python event-time stateful stream engine implementing the exact same versioned anomaly rules, bounded out-of-order watermarks, keyed state with TTL, event deduplication, gap detection, late-event side outputs, and atomic JSON/SQLite checkpoint snapshots as the PyFlink `2.1.3` job.
     - **Lakehouse Storage:** Real **Apache Iceberg Format V2** tables (`metadata.json`, Avro manifests, Parquet files, real snapshots, time travel, schema evolution, compaction) managed by `pyiceberg==0.12.0` + SQLite catalog.
     - **Batch & SQL Analytics:** Real Arrow/DuckDB 2.0.0 batch execution and Iceberg SQL queries replicating the PySpark 4.1.4 and Trino 484 pipelines.
   - All benchmark reports, test summaries, and UI banners explicitly record which runtime profile produced each measurement. No local fallback result is ever misrepresented as a distributed JVM cluster measurement.

## 5. Frontend & Geospatial Map Licensing

- **MapLibre GL JS** (BSD-3-Clause) and **Chart.js** (MIT) are open-source and require zero API keys or commercial map tokens.
- To avoid violating public OpenStreetMap tile server usage policies and ensure offline reproducibility, the dashboard renders a **self-hosted GeoJSON vector basemap** served directly from FastAPI (`/static/tiles/metro_basemap.geojson`) with geofence polygons and H3 hexagon overlays—requiring zero external tile fetches.
