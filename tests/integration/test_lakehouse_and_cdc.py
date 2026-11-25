"""Integration tests for Apache Iceberg Format V2 features (time travel, schema evolution,
partition evolution, compaction, expiration, orphan safety) and CDC SCD Type 2 maintenance.
"""

from __future__ import annotations

import pytest
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import StringType

from fleet_platform.cdc import (
    CdcReferencePipeline,
    build_scd2_dimension,
    generate_synthetic_cdc_events,
)
from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.simulator import FleetSimulator, SimulationConfig
from fleet_platform.transport.local_wal import LocalWalEventBus


@pytest.mark.integration
def test_iceberg_time_travel_schema_partition_evolution_and_compaction(
    isolated_config: PlatformConfig,
) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    lh = IcebergLakehouse(isolated_config)
    ingestor = BronzeIngestor(isolated_config, bus, reg, lh)

    # Batch 1 -> Snapshot 1
    FleetSimulator(
        isolated_config,
        bus,
        reg,
        SimulationConfig(
            virtual_fleet_size=20, active_vehicles=20, ticks=2, seed=10, poison_count=0
        ),
    ).run()
    ingestor.drain_all_topics()
    tbl = lh.load_table("bronze.telemetry_raw")
    snap1_id = tbl.current_snapshot().snapshot_id
    cnt1 = len(lh.read_arrow("bronze.telemetry_raw", snapshot_id=snap1_id))

    # Batch 2 -> Snapshot 2 (creates a second Parquet file)
    FleetSimulator(
        isolated_config,
        bus,
        reg,
        SimulationConfig(
            virtual_fleet_size=20, active_vehicles=20, ticks=2, seed=11, poison_count=0
        ),
    ).run()
    ingestor.drain_all_topics()
    tbl = lh.load_table("bronze.telemetry_raw")
    snap2_id = tbl.current_snapshot().snapshot_id
    cnt2 = len(lh.read_arrow("bronze.telemetry_raw", snapshot_id=snap2_id))
    assert snap2_id != snap1_id
    assert cnt2 > cnt1

    # Verify Iceberg Time Travel returns exact Snapshot 1 row count
    tt_rows = lh.query_sql(
        "SELECT COUNT(*) AS cnt FROM bronze_telemetry",
        snapshot_overrides={"bronze.telemetry_raw": snap1_id},
    )[0]["cnt"]
    assert tt_rows == cnt1

    # Verify Small-File Compaction merges >=2 Parquet files into 1 while preserving exact rows
    before_files = lh.table_stats("bronze.telemetry_raw")["data_file_count"]
    assert before_files >= 2
    comp = lh.compact_table("bronze.telemetry_raw")
    assert comp["action"] == "compacted"
    assert comp["files_after"] == 1
    assert comp["records_preserved"] == cnt2

    # Verify Iceberg Schema Evolution (adding an optional column without rewriting historical data)
    with tbl.update_schema() as update:
        update.add_column("fleet_operator_note", StringType(), doc="Schema evolution test column")
    evolved = lh.load_table("bronze.telemetry_raw")
    assert "fleet_operator_note" in [f.name for f in evolved.schema().fields]
    assert len(lh.read_arrow("bronze.telemetry_raw")) == cnt2

    # Verify Iceberg Partition Evolution (adding region identity partition field)
    with evolved.update_spec() as update_spec:
        update_spec.add_field("region", IdentityTransform(), "region_part")
    spec_fields = [f.name for f in lh.load_table("bronze.telemetry_raw").spec().fields]
    assert "region_part" in spec_fields

    # Verify Orphan-File Audit respects safety window and leaves referenced files untouched
    orphan_report = lh.detect_orphan_files("bronze.telemetry_raw", min_age_hours=72.0, delete=False)
    assert orphan_report["referenced_files"] >= 1
    assert orphan_report["deleted"] == 0


@pytest.mark.integration
def test_cdc_lsn_ordering_dedup_deletes_and_scd2_invariants(
    isolated_config: PlatformConfig,
) -> None:
    events = generate_synthetic_cdc_events(
        num_vehicles=25, seed=7, include_updates=True, include_duplicates=True
    )
    scd2_rows = build_scd2_dimension(events)

    # Every vehicle has at most ONE is_current=True row
    current_by_veh: dict[str, int] = {}
    for row in scd2_rows:
        if row["is_current"]:
            current_by_veh[row["vehicle_id"]] = current_by_veh.get(row["vehicle_id"], 0) + 1
    assert all(c == 1 for c in current_by_veh.values())

    # Deleted vehicle VEH-000025 must have is_current=False and non-null effective_to
    v25_rows = [r for r in scd2_rows if r["vehicle_id"] == "VEH-000025"]
    assert len(v25_rows) == 1
    assert v25_rows[0]["is_current"] is False
    assert v25_rows[0]["effective_to"] is not None

    # End-to-end CDC topic -> Iceberg silver.dim_vehicle_scd2
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    lh = IcebergLakehouse(isolated_config)
    cdc = CdcReferencePipeline(isolated_config, bus, reg, lh)
    published = cdc.publish_synthetic_cdc(num_vehicles=25, seed=7)
    assert published > 25
    res = cdc.materialize_scd2()
    assert res["current_rows"] == 24
    assert res["historical_rows"] >= 2
