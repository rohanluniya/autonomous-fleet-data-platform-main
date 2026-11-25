# Data Retention, Deletion Policy & Dataset Ownership Catalog

## 1. Retention & Lifecycle Schedule

| Tier / Store | Dataset Scope | Retention Window | Enforcement Mechanism |
| :--- | :--- | :---: | :--- |
| **Kafka / WAL Topics** | `fleet.telemetry.raw`, `fleet.gps.raw` | 7 days | Broker `retention.ms=604800000` |
| **Kafka / WAL Topics** | `fleet.diagnostics.raw` | 14 days | Broker `retention.ms=1209600000` |
| **Kafka / WAL Topics** | `fleet.trips.raw`, `fleet.autonomy.raw`, `fleet.anomalies.detected`, `fleet.telemetry.dlq` | 30 days | Broker `retention.ms=2592000000` |
| **Iceberg Bronze** | `bronze.*` | 90 days (keep `>= 5` snapshots / 7d time travel) | Nightly `fleet_iceberg_maintenance` DAG |
| **Iceberg Silver** | `silver.*` | 365 days | Partition expiration + compaction |
| **Iceberg Gold** | `gold.*` | 730 days | Partition expiration |
| **Orphan Parquet Files** | Unreferenced files in object storage | Deleted ONLY after `>= 72 hours` age check | `detect_orphan_files(min_age_hours=72.0)` / Spark `remove_orphan_files` |

## 2. Synthetic Identity Privacy & Targeted Deletion

- All vehicle IDs (`VEH-000001`..`VEH-1000000`) and location traces are synthetic; no real driver or customer PII is collected.
- Public and analyst API responses automatically coarsen coordinates to H3 resolution 6 (`~36 km^2`). Raw coordinates require `operator` or `admin` bearer tokens and emit `audit.raw_location_read` JSON audit entries.
- Targeted vehicle erasure (GDPR/CCPA pattern readiness) is executed via Iceberg Format V2 row-level deletes (`DELETE FROM fleet.silver.telemetry_clean WHERE vehicle_id = '...'`) followed by snapshot expiration after retention grace period.

## 3. Dataset Ownership Catalog

| Domain | Tables / Topics | Owner Team | Classification |
| :--- | :--- | :--- | :--- |
| **Raw Telemetry Ingestion** | `fleet.*.raw`, `bronze.*` | `data-platform-ingestion` | Internal-Restricted (raw GPS) |
| **Streaming Intelligence** | `fleet.anomalies.detected`, `fleet.vehicle-state`, `silver.anomalies_verified` | `data-platform-streaming` | Internal-Operational |
| **Curated Fleet Analytics** | `silver.telemetry_clean`, `silver.trips_reconstructed`, `silver.dim_vehicle_scd2`, `gold.*` | `data-platform-analytics` | Internal-Coarsened (Gold) |
| **Governance & Quality** | `quality.dq_check_results`, `bronze.telemetry_dlq`, `silver.telemetry_quarantine` | `data-platform-quality` | Internal-Operational |
