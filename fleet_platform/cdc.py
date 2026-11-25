"""CDC reference-data pipeline & SCD Type 2 Vehicle Dimension builder.

Processes Debezium-compatible PostgreSQL CDC envelopes (snapshot 'r', create 'c',
update 'u', delete 'd'), deduplicates duplicate LSN deliveries, orders changes
deterministically by (lsn, ts_ms), and builds SCD Type 2 rows in `silver.dim_vehicle_scd2`
only for meaningful dimensional attributes (hardware_generation, vehicle_model,
assigned_region, software_version, fleet_tier, maintenance_status).
"""

from __future__ import annotations

import hashlib
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa

from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.geospatial.spatial import METRO_REGIONS
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.observability import OpenLineageEmitter, get_logger
from fleet_platform.simulator import SOFTWARE_VERSIONS, VEHICLE_MODELS
from fleet_platform.transport.base import EventBus

SCD2_TRACKED_FIELDS = (
    "hardware_generation",
    "vehicle_model",
    "assigned_region",
    "software_version",
    "fleet_tier",
    "maintenance_status",
)


def deterministic_vehicle_sk(vehicle_id: str, effective_from_iso: str) -> str:
    digest = hashlib.sha256(f"{vehicle_id}|{effective_from_iso}".encode()).hexdigest()
    return f"VSK-{digest[:16]}"


def generate_synthetic_cdc_events(
    *,
    num_vehicles: int,
    seed: int = 42,
    base_time: datetime | None = None,
    include_updates: bool = True,
    include_duplicates: bool = True,
) -> list[dict[str, Any]]:
    """Generate deterministic Debezium-shaped CDC snapshot + firmware/region/delete events."""
    rng = random.Random(seed)
    t0 = base_time or datetime(2026, 11, 24, 0, 0, 0, tzinfo=UTC)
    regions = list(METRO_REGIONS.keys())
    events: list[dict[str, Any]] = []
    lsn = 1000

    # 1. Initial snapshot ('r') for every vehicle
    for idx in range(1, num_vehicles + 1):
        vid = f"VEH-{idx:06d}"
        lsn += 1
        events.append(
            {
                "cdc_event_id": f"cdc-snap-{idx:06d}",
                "op": "r",
                "ts_ms": int(t0.timestamp() * 1000),
                "lsn": lsn,
                "tx_id": 100,
                "vehicle_id": vid,
                "hardware_generation": "HW-Gen3" if idx % 3 == 0 else "HW-Gen4",
                "vehicle_model": VEHICLE_MODELS[(idx // 3) % len(VEHICLE_MODELS)],
                "assigned_region": regions[idx % len(regions)],
                "software_version": SOFTWARE_VERSIONS[(idx // 9) % len(SOFTWARE_VERSIONS)],
                "fleet_tier": "commercial_metro" if idx % 5 != 0 else "validation_cohort",
                "maintenance_status": "ACTIVE",
                "schema_version": "1.0.0",
            }
        )

    # 2. Meaningful SCD2 changes: OTA firmware upgrades & region reassignments
    if include_updates:
        upgrade_time = t0 + timedelta(hours=12, minutes=1)
        upgrade_targets = list(range(1, min(num_vehicles + 1, max(6, num_vehicles // 10 + 2))))
        for idx in upgrade_targets:
            vid = f"VEH-{idx:06d}"
            lsn += 1
            ev = {
                "cdc_event_id": f"cdc-ota-{idx:06d}",
                "op": "u",
                "ts_ms": int(upgrade_time.timestamp() * 1000),
                "lsn": lsn,
                "tx_id": 200 + idx,
                "vehicle_id": vid,
                "hardware_generation": "HW-Gen3" if idx % 3 == 0 else "HW-Gen4",
                "vehicle_model": VEHICLE_MODELS[idx % len(VEHICLE_MODELS)],
                "assigned_region": regions[idx % len(regions)],
                "software_version": "v4.3.0-rc1",
                "fleet_tier": "commercial_metro" if idx % 5 != 0 else "validation_cohort",
                "maintenance_status": "ACTIVE",
                "schema_version": "1.0.0",
            }
            events.append(ev)
            if include_duplicates and idx <= 3:
                # Duplicate CDC delivery with identical LSN (must be suppressed idempotently)
                events.append(dict(ev))

        # Simulate a vehicle decommission delete ('d') for the last vehicle when >= 10 vehicles
        if num_vehicles >= 10:
            del_time = t0 + timedelta(hours=13)
            lsn += 1
            events.append(
                {
                    "cdc_event_id": f"cdc-del-{num_vehicles:06d}",
                    "op": "d",
                    "ts_ms": int(del_time.timestamp() * 1000),
                    "lsn": lsn,
                    "tx_id": 999,
                    "vehicle_id": f"VEH-{num_vehicles:06d}",
                    "hardware_generation": None,
                    "vehicle_model": None,
                    "assigned_region": None,
                    "software_version": None,
                    "fleet_tier": None,
                    "maintenance_status": "DECOMMISSIONED",
                    "schema_version": "1.0.0",
                }
            )

    # Shuffle a tiny subset to prove LSN ordering restores transaction consistency
    if len(events) > 5:
        rng.shuffle(events[-4:])
    return events


def build_scd2_dimension(cdc_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build deterministic SCD Type 2 rows from raw CDC events with LSN ordering & dedup."""
    # Deduplicate by (vehicle_id, lsn, op) and sort strictly by transaction order (lsn, ts_ms)
    seen_keys: set[tuple[str, int, str]] = set()
    ordered: list[dict[str, Any]] = []
    for ev in sorted(cdc_events, key=lambda x: (int(x["lsn"]), int(x["ts_ms"]))):
        key = (str(ev["vehicle_id"]), int(ev["lsn"]), str(ev["op"]))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ordered.append(ev)

    history_by_vehicle: dict[str, list[dict[str, Any]]] = {}
    for ev in ordered:
        vid = str(ev["vehicle_id"])
        op = str(ev["op"])
        ts_dt = datetime.fromtimestamp(int(ev["ts_ms"]) / 1000.0, tz=UTC).replace(tzinfo=None)
        rows = history_by_vehicle.setdefault(vid, [])

        if op == "d":
            if rows and rows[-1]["is_current"]:
                rows[-1]["effective_to"] = ts_dt
                rows[-1]["is_current"] = False
            continue

        new_attrs = {
            "hardware_generation": ev.get("hardware_generation") or "HW-Gen4",
            "vehicle_model": ev.get("vehicle_model") or "AV-Sedan-G3",
            "assigned_region": ev.get("assigned_region") or "sf_bay",
            "software_version": ev.get("software_version") or "v4.2.0",
            "fleet_tier": ev.get("fleet_tier") or "commercial_metro",
            "maintenance_status": ev.get("maintenance_status") or "ACTIVE",
        }

        if rows and rows[-1]["is_current"]:
            prev = rows[-1]
            # Ignore no-op updates where no tracked SCD2 attribute actually changed
            if all(prev[f] == new_attrs[f] for f in SCD2_TRACKED_FIELDS):
                continue
            prev["effective_to"] = ts_dt
            prev["is_current"] = False

        rows.append(
            {
                "vehicle_sk": deterministic_vehicle_sk(vid, ts_dt.isoformat()),
                "vehicle_id": vid,
                **new_attrs,
                "effective_from": ts_dt,
                "effective_to": None,
                "is_current": True,
                "cdc_lsn": int(ev["lsn"]),
            }
        )

    flattened: list[dict[str, Any]] = []
    for vid in sorted(history_by_vehicle):
        flattened.extend(history_by_vehicle[vid])
    return flattened


class CdcReferencePipeline:
    """Publishes/consumes CDC events via event bus and materializes `silver.dim_vehicle_scd2`."""

    CONSUMER_GROUP = "fleet-cdc-scd2-builder"

    def __init__(
        self,
        config: PlatformConfig,
        bus: EventBus,
        registry: AvroContractRegistry,
        lakehouse: IcebergLakehouse,
        lineage: OpenLineageEmitter | None = None,
    ) -> None:
        self.config = config
        self.bus = bus
        self.registry = registry
        self.lakehouse = lakehouse
        self.lineage = lineage or OpenLineageEmitter(
            namespace=config.openlineage_namespace,
            marquez_url=config.openlineage_url,
            local_store_dir=config.lineage_dir,
        )
        self.logger = get_logger("cdc-pipeline", "scd2_builder", environment=config.env)

    def publish_synthetic_cdc(self, num_vehicles: int, seed: int = 42) -> int:
        events = generate_synthetic_cdc_events(num_vehicles=num_vehicles, seed=seed)
        batch = [
            (
                str(ev["vehicle_id"]),
                self.registry.serialize("fleet.cdc.vehicle-registry", ev),
                int(ev["ts_ms"]),
                {"cdc_op": str(ev["op"])},
            )
            for ev in events
        ]
        return self.bus.produce_batch("fleet.cdc.vehicle-registry", batch)

    def materialize_scd2(self) -> dict[str, Any]:
        """Read all CDC records from the topic, reconstruct SCD2 dimension, and atomically commit."""
        records = self.bus.read_range("fleet.cdc.vehicle-registry", max_records=2_000_000)
        decoded_events = []
        for r in records:
            _, ev = self.registry.deserialize(r.value, expected_topic="fleet.cdc.vehicle-registry")
            decoded_events.append(ev)

        scd2_rows = build_scd2_dimension(decoded_events)
        tbl = self.lakehouse.load_table("silver.dim_vehicle_scd2")
        arrow_tbl = pa.Table.from_pylist(scd2_rows, schema=tbl.schema().as_arrow())
        self.lakehouse.overwrite_arrow("silver.dim_vehicle_scd2", arrow_tbl)

        current_cnt = sum(1 for r in scd2_rows if r["is_current"])
        hist_cnt = len(scd2_rows) - current_cnt
        self.lineage.emit(
            event_type="COMPLETE",
            job_name="cdc_scd2_vehicle_dimension",
            inputs=[{"name": "fleet.cdc.vehicle-registry"}],
            outputs=[{"name": "silver.dim_vehicle_scd2", "row_count": len(scd2_rows)}],
        )
        self.logger.info(
            "cdc.scd2_materialized",
            f"Materialized {len(scd2_rows)} SCD2 rows ({current_cnt} current, {hist_cnt} historical)",
            context={"cdc_events_read": len(decoded_events), "scd2_rows": len(scd2_rows)},
        )
        return {
            "cdc_events_read": len(decoded_events),
            "scd2_total_rows": len(scd2_rows),
            "current_rows": current_cnt,
            "historical_rows": hist_cnt,
        }
