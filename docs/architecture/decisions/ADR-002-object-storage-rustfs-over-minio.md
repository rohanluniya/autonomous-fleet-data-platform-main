# ADR-002: S3-Compatible Local Object Storage — RustFS Over MinIO CE

- **Status:** Accepted
- **Scope:** Local and Kubernetes open-source object storage for Apache Iceberg Parquet files and Flink checkpoints

## Context
For years, MinIO Community Edition (`minio/minio`) was the default S3-compatible local storage container for Iceberg lakehouse development. During pre-pinning compatibility verification, we confirmed critical upstream lifecycle changes:
- In May 2025, MinIO removed key console and administration features from the Community Edition.
- In October 2025, MinIO halted publication of free pre-built binaries and public Docker Hub images, leaving unpatched CVEs in older tags.
- On April 25, 2026, the MinIO Community Edition repository was formally archived (read-only).

## Options Considered
1. **RustFS (`rustfs/rustfs:1.0.6`)** — Apache-2.0 licensed, actively maintained Rust S3-compatible object store with official multi-arch (`amd64`, `arm64`) Docker Hub images, AWS SigV4 support, S3 API on port `9000`, and web console on port `9001`.
2. **Garage** — AGPLv3 lightweight S3 store designed for geo-distributed edge setups; non-trivial cluster layout bootstrapping for single-node local dev.
3. **SeaweedFS** — Apache-2.0, capable but requires separate master/volume/filer/s3 topology wiring.
4. **Archived MinIO tags** — Unmaintained and fails our open-source security & supply-chain standards.

## Decision
Adopt **RustFS `1.0.6`** (`rustfs/rustfs:1.0.6`, Apache-2.0) as the S3-compatible object store for Docker Compose and Kubernetes deployments. Configure Spark (`S3FileIO`), Flink (`flink-s3-fs-hadoop`), Trino (`fs.native-s3.enabled=true`), and Lakekeeper with `path-style-access=true` pointing to `http://rustfs:9000`.

## Consequences
- **Positive:** Commercial-friendly Apache-2.0 license, active upstream maintenance, native Apple Silicon (`arm64`) + Linux (`amd64`) containers, low memory overhead, and standard S3 wire compatibility.
- **Tradeoff:** RustFS is newer than legacy MinIO; therefore, integration tests explicitly verify multipart uploads, Iceberg manifest atomic reads, and S3 bucket lifecycle operations.
