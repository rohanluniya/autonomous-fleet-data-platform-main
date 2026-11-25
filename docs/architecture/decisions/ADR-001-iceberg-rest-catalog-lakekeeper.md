# ADR-001: Single Authoritative Iceberg REST Catalog (Lakekeeper)

- **Status:** Accepted
- **Scope:** Lakehouse metadata & multi-engine interoperability (Spark, Flink, Trino, PyIceberg)

## Context
An Apache Iceberg lakehouse requires a single atomic catalog to coordinate snapshot commits across Apache Spark, Apache Flink, Trino, and Python utilities (`pyiceberg`). Legacy Hive Metastore (HMS) requires heavy Thrift/JVM dependencies and lacks modern credential vending and multi-table REST commit standards. Meanwhile, running disparate catalogs (e.g., Hadoop catalog for Spark and JDBC for Trino) breaks snapshot atomicity and risks silent metadata corruption.

## Options Considered
1. **Lakekeeper (`quay.io/lakekeeper/catalog:v0.15.1`)** — Apache-2.0, Rust single-binary implementation of the Apache Iceberg REST Catalog specification backed by PostgreSQL; native multi-arch (`amd64` + `arm64`), ~30–60 MB RSS footprint, fast bootstrap, optional OpenFGA/Cedar authorization.
2. **Apache Polaris (`apache/polaris:1.9.0`)** — Apache-2.0 ASF top-level Java/Quarkus REST catalog with rich enterprise RBAC and multi-tenant governance, but higher JVM memory baseline (~512 MB–1 GB RSS) on local developer laptops.
3. **Hive Metastore (HMS)** — Mature, but heavyweight JVM + Thrift surface area and non-REST protocol.

## Decision
Select **Lakekeeper `v0.15.1`** as the single authoritative Iceberg REST Catalog for Docker Compose and Kubernetes deployments, backed by PostgreSQL. In zero-Docker local execution (`local_native` profile), use **`pyiceberg` SQLite Catalog** (`sqlite:///.local_data/iceberg_catalog.db`), which implements the exact same Iceberg Format V2 table specification (`metadata.json`, Avro manifests, Parquet files) without requiring a container runtime.

## Consequences
- **Positive:** Spark 4.1.4, Flink 2.1.3, Trino 484, and PyIceberg 0.12.0 all speak the identical Iceberg REST protocol to one source of truth; minimal memory overhead leaves headroom for Flink, Spark, and Trino locally.
- **Tradeoff:** Enterprises requiring ASF-governed multi-tenant principal federation out of the box can swap the REST URI to **Apache Polaris `1.9.0`** without changing Spark, Flink, Trino, or application SQL because all clients target the standard Iceberg REST spec.
