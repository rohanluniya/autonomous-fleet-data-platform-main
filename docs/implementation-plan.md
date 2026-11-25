# Implementation Plan & Acceptance-Test Matrix

All 15 engineering phases and mandatory acceptance criteria (A–O) have been implemented and verified under the `local_native` execution profile, with Docker Compose (`core`, `analytics`, `observability`, `full`), Kubernetes manifests, and GitHub Actions CI validated statically and ready for container hosts.

## Phase Progress Tracker

| Phase | Scope | Status | Verification Evidence |
| :--- | :--- | :---: | :--- |
| **Phase 0** | Research, Architecture, ADRs, Compatibility Matrix, Repo Bootstrap | **Completed** | `docs/compatibility.md`, `docs/architecture/`, `ADR-001..006`, `pyproject.toml`, `uv.lock` |
| **Phase 1** | Deterministic Synthetic Fleet Simulator + Avro Contracts & Compat Tests | **Completed** | `fleet_platform/simulator.py`, `schemas/avro/`, `tests/contracts/test_avro_contracts.py` |
| **Phase 2** | Kafka Topology, Partitioning, Validation, DLQ, Replay Utility | **Completed** | `fleet_platform/transport/`, `fleet_platform/ingestion.py`, `services/ingestion/` |
| **Phase 3** | Apache Flink 2.1.3 Stateful Streaming + Anomaly Rules + Checkpoints | **Completed** | `fleet_platform/streaming/`, `services/stream-processing/flink_jobs/` |
| **Phase 4** | Apache Iceberg Format V2 Medallion Lakehouse (Bronze/Silver/Gold + SCD2) | **Completed** | `fleet_platform/lakehouse/iceberg_store.py`, `tests/integration/test_lakehouse_and_cdc.py` |
| **Phase 5** | Apache Spark 4.1.4 Batch Jobs, Backfills, Reconciliation & Optimizations | **Completed** | `pipelines/spark/`, `benchmarks/lakehouse_optimizations.py`, `results/benchmarks/lakehouse_optimizations.json` |
| **Phase 6** | Geospatial Intelligence (H3, Shapely Geofences, Hotspots, PostGIS Serving) | **Completed** | `fleet_platform/geospatial/spatial.py`, `pipelines/cdc/02_postgis_serving.sql` |
| **Phase 7** | PostgreSQL Operational Registry + Debezium 3.7.1 CDC Pipeline | **Completed** | `fleet_platform/cdc.py`, `pipelines/cdc/01_vehicle_registry.sql`, `debezium-postgres-connector.json` |
| **Phase 8** | Apache Airflow 3.4.0 DAGs + Data Quality & Quarantine Framework | **Completed** | `pipelines/airflow/dags/fleet_platform_dags.py`, `fleet_platform/quality/validator.py` |
| **Phase 9** | Observability (JSON Logs, Prometheus, OpenTelemetry, Grafana, OpenLineage) | **Completed** | `fleet_platform/observability/`, `infra/observability/` |
| **Phase 10** | Analytics SQL (Trino / DuckDB) + FastAPI Serving + MapLibre Dashboard | **Completed** | `apps/fleet-api/`, `apps/fleet-dashboard/`, `sql/trino/`, `docs/assets/dashboard_live.png` |
| **Phase 11** | Security, RBAC, Audit Logging, SBOM, License Checks, Retention | **Completed** | `apps/fleet-api/security.py`, `scripts/generate_sbom.py`, `scripts/check_licenses.py`, `results/security/sbom.cdx.json` |
| **Phase 12** | Controlled Fault Injection & Incident Reports | **Completed** | `tests/fault/test_fault_injection.py`, `results/fault_injection/fault_injection_results.json`, `docs/incident-reports/INC-001-*.md` |
| **Phase 13** | Sustained Benchmarking (smoke/local/stress/1M-virtual) & Plots | **Completed** | `benchmarks/run_benchmarks.py`, `results/benchmarks/sustained_benchmarks.{json,csv}`, `docs/assets/*.png` |
| **Phase 14** | Docker Compose Profiles, Kubernetes/Helm Manifests & GitHub Actions CI | **Completed** | `infra/docker/docker-compose.yml`, `infra/kubernetes/fleet-platform.yaml`, `.github/workflows/` |
| **Phase 15** | Portfolio Documentation, README, APPENDIX, Walkthrough & Final Audit | **Completed** | `README.md`, `APPENDIX.md`, `docs/interview-deep-dive.md`, `docs/engineering-case-study.md`, `docs/limitations.md` |

## Mandatory Acceptance-Test Matrix (A–O)

| ID | Acceptance Criterion | Verification Result | Evidence |
| :---: | :--- | :---: | :--- |
| **A** | Fresh setup boots the core platform | **PASS (`local_native`)** | `make setup && make detect-env && make run-demo` verified cleanly; Docker Compose pinned & validated statically. |
| **B** | Generate 1,000 synthetic vehicles and observe ingestion | **PASS** | `local` profile: 15,224 events produced at 56,421 events/sec across 12 WAL partitions. |
| **C** | Events reach durable Bronze Iceberg storage and can be queried | **PASS** | Real Iceberg Format V2 `bronze.telemetry_raw` Parquet + metadata queried via DuckDB 2.0. |
| **D** | Silver normalization and Gold aggregates are correct | **PASS** | 11/11 Data Quality checks PASS; 0 Bronze-to-Silver and Silver-to-Gold row discrepancy. |
| **E** | Inject known hard-braking anomaly and verify detection with evidence | **PASS** | `VEH-000001` triggered 3x `RULE_HARD_BRAKING` + `RULE_REPEATED_HARD_BRAKING_CEP` (CRITICAL) with evidence & source `event_id`s. |
| **F** | Inject duplicate & out-of-order messages and verify documented behavior | **PASS** | `test_fault_03`: online dedup suppressed duplicate; late event routed to side output and reconciled in Silver. |
| **G** | Restart streaming processor and verify checkpoint recovery | **PASS** | `test_fault_02`: recovered from mid-stream crash in `8.36 ms` with exact `468/468` processed and `10/10` anomalies parity. |
| **H** | Replay bounded dataset and verify idempotent business results | **PASS** | `test_fault_05` & E2E test: double Bronze replay yields identical Gold totals. |
| **I** | Run batch backfill and verify reconciliation | **PASS** | `run_backfill(target_date="2026-11-24")` reconciled with `11/11` DQ checks PASS. |
| **J** | Query Gold datasets through analytical SQL engine | **PASS (`DuckDB`)** | Snapshot-isolated SQL queries verified on DuckDB 2.0; Trino 484 catalog & SQL queries in `sql/trino/`. |
| **K** | Access meaningful API endpoints and dashboard views | **PASS** | All 9 FastAPI endpoints HTTP 200; RBAC verified; live Chrome screenshot saved to `docs/assets/dashboard_live.png`. |
| **L** | Observe real operational metrics and structured logs | **PASS** | 221-line Prometheus `/metrics`, secret-scrubbed JSON logs, OTel spans, OpenLineage v2 events verified. |
| **M** | Run controlled failure injection and produce incident report | **PASS** | 6/6 fault scenarios passed (`results/fault_injection/fault_injection_results.json`) + `INC-001` report written. |
| **N** | Execute automated test suite and publish actual pass/fail report | **PASS** | `18 passed, 0 failed` (`results/tests/pytest_report.xml`). |
| **O** | Run sustained benchmark and publish raw results with hardware specs | **PASS** | `results/benchmarks/sustained_benchmarks.{json,csv}` & `lakehouse_optimizations.json` + plots. |
