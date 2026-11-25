# Event Contracts, Avro Schemas & Topic Topology

## 1. Registered Topics & Contracts

All event schemas live in `schemas/avro/` and use 5-byte Confluent/Apicurio wire framing (`0x00` magic byte + big-endian 4-byte `schema_id` + binary Avro payload).

| Topic | Partitions | Key | Retention | Compression | Schema File | Schema ID |
| :--- | :---: | :--- | :---: | :---: | :--- | :---: |
| `fleet.telemetry.raw` | 12 | `vehicle_id` | 7 days | `zstd` | `schemas/avro/telemetry_event.avsc` | `1` |
| `fleet.gps.raw` | 12 | `vehicle_id` | 7 days | `zstd` | `schemas/avro/gps_event.avsc` | `2` |
| `fleet.diagnostics.raw` | 6 | `vehicle_id` | 14 days | `zstd` | `schemas/avro/diagnostics_event.avsc` | `3` |
| `fleet.trips.raw` | 6 | `vehicle_id` | 30 days | `zstd` | `schemas/avro/trip_event.avsc` | `4` |
| `fleet.autonomy.raw` | 6 | `vehicle_id` | 30 days | `zstd` | `schemas/avro/autonomy_event.avsc` | `5` |
| `fleet.anomalies.detected` | 6 | `vehicle_id` | 30 days | `zstd` | `schemas/avro/anomaly_event.avsc` | `6` |
| `fleet.vehicle-state` | 6 | `vehicle_id` | 30d + compact | `zstd` | `schemas/avro/vehicle_state_event.avsc` | `7` |
| `fleet.cdc.vehicle-registry` | 6 | `vehicle_id` | 30 days | `zstd` | `schemas/avro/cdc_vehicle_registry.avsc` | `8` |
| `fleet.telemetry.dlq` | 3 | `vehicle_id` | 30 days | `zstd` | `schemas/avro/dlq_event.avsc` | `9` |

## 2. Schema Evolution & Compatibility Policy

- **Enforced Mode:** `FULL` compatibility (both backward and forward compatible) across minor/additive revisions.
- **Verified Evolution Pair:** `schemas/avro/telemetry_event.avsc` (`1.0.0`) <-> `schemas/avro/v2/telemetry_event.avsc` (`2.0.0`), which adds nullable `ambient_temp_c` and `tire_pressure_psi` fields with `"default": null`.
- **Automated Verification:** `tests/contracts/test_avro_contracts.py` verifies:
  1. v2 readers cleanly decode v1 payloads (backward compatibility).
  2. v1 readers cleanly decode v2 payloads (forward compatibility).
  3. Adding a required field without a default is actively rejected before merge.
- **Registry Governance:** Docker deployments register all schemas against **Apicurio Registry `3.3.5`** (Apache-2.0) at `/apis/ccompat/v7`.

## 3. Poison-Message Quarantine Contract

Any byte payload failing wire-header checks, schema ID validation, or Avro deserialization is immediately wrapped in `DlqEvent` (`schema_id=9`) with `source_topic`, `source_partition`, `source_offset`, `error_code`, `error_message`, `quarantined_at`, and `raw_payload_b64`, emitted to `fleet.telemetry.dlq`, and appended to `bronze.telemetry_dlq`. Consumers never stall or silently discard malformed records.
