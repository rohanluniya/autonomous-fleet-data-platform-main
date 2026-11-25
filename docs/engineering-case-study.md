# Engineering Case Study: Designing a Verifiable Autonomous Fleet Data Platform

## 1. The Architectural Problem
Autonomous mobility fleets produce heterogeneous, bursty, geographically distributed telemetry over unreliable cellular links. Vehicles enter tunnels, buffer out-of-order messages, emit duplicate frames on reconnect, undergo OTA firmware rollouts, and occasionally transmit corrupt sensor frames. A naive pipeline that drops late messages, relies on broker arrival time, skips schema enforcement, or publishes raw GPS points directly to dashboards produces misleading reliability KPIs and creates operational security leaks.

## 2. Core Architectural Decisions & Tradeoffs

1. **Separating Real-Time Streaming (Flink) from Batch Lakehouse Curation (Spark):**
   - Low-latency anomaly detection (`RULE_REPEATED_HARD_BRAKING_CEP`: `>=3` hard brakes within `120s`) and telemetry-gap detection require keyed event-time state and watermarks.
   - Historical SCD Type 2 joins (`silver.dim_vehicle_scd2`), trip reconstruction, multi-day backfills, and Iceberg file compaction belong in batch compute so maintenance never impacts alerting latency and rule upgrades never halt raw Bronze ingestion (`ADR-004`).
2. **Rejecting "End-to-End Exactly-Once" Marketing:**
   - Instead of pretending Kafka + Flink + Spark + Iceberg share a global two-phase commit, we enforce deterministic UUIDv5 `event_id` keys at the source, commit consumer offsets strictly *after* atomic Bronze Iceberg appends, deduplicate deterministically in Silver, and overwrite Gold partitions atomically. Controlled fault injection proved zero duplicate Gold counts even after double-replaying Bronze (`INC-001`).
3. **Responding to Real Ecosystem Shifts (MinIO Archival & Multi-Arch Apple Silicon):**
   - Verifying upstream releases before pinning revealed that MinIO CE stopped publishing free Docker images in October 2025 and was archived in April 2026 (`ADR-002`), and upstream `postgis/postgis` still lagged on native `arm64` manifests. Selecting **RustFS `1.0.6`** (Apache-2.0), **Lakekeeper `v0.15.1`** (Apache-2.0), **Apicurio Registry `3.3.5`** (Apache-2.0), and **`imresamu/postgis:17-3.6.1-alpine3.23`** produced a strictly OSI-open-source, native Apple Silicon + x86_64 stack.
4. **Confronting the 1,000,000-Vehicle State Trap:**
   - Our analytical capacity model (`docs/capacity-model.md`) exposed that naive 24-hour `event_id` deduplication in Flink at 1.15M events/sec requires ~4.77 TB of RocksDB state. Production architecture bounds Flink online dedup to short reconnect windows (or O(1) sequence watermarks) and delegates multi-day dedup to partition-pruned Spark `MERGE` in Silver.
5. **Privacy-By-Default Geospatial Serving:**
   - Exact vehicle trajectories are sensitive operational intelligence. FastAPI RBAC enforces server-side H3 resolution-6 coarsening (`~36 km^2`) for default/analyst views, restricts exact GPS coordinates to audited `operator`/`admin` calls, and renders maps using a self-hosted GeoJSON vector layer without third-party tile dependencies.

## 3. Verification Outcomes
Rather than leaving diagrams untested, the platform executed end-to-end across 1,000 active vehicles (`11/11` Data Quality checks passing), scaled locally to 10,000 active vehicles and 1,000,000 virtual vehicle IDs, proved `3.84×–6.00×` Iceberg read speedups from partitioning, compaction, and pushdown, passed all 6 controlled fault-injection drills (`8.36 ms` checkpoint recovery with exact anomaly parity), and produced live operational dashboards and CycloneDX SBOM artifacts.
