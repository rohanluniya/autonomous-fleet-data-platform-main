"""Unit tests for deterministic fleet simulation, Murmur2 partitioning, and geospatial logic."""

from __future__ import annotations

import pytest

from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.geospatial.spatial import (
    coarsen_location,
    haversine_km,
    match_geofences,
    route_deviation_meters,
    to_h3_cell,
    validate_gps,
)
from fleet_platform.simulator import FleetSimulator, SimulationConfig, deterministic_event_id
from fleet_platform.transport.base import murmur2_partition
from fleet_platform.transport.local_wal import LocalWalEventBus


@pytest.mark.unit
def test_deterministic_event_ids_and_replay(isolated_config: PlatformConfig) -> None:
    id1 = deterministic_event_id(42, "VEH-000001", "telemetry", 5)
    id2 = deterministic_event_id(42, "VEH-000001", "telemetry", 5)
    id3 = deterministic_event_id(43, "VEH-000001", "telemetry", 5)
    assert id1 == id2
    assert id1 != id3


@pytest.mark.unit
def test_virtual_fleet_1m_vs_active_subset(isolated_config: PlatformConfig) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    sim_cfg = SimulationConfig(
        virtual_fleet_size=1_000_000,
        active_vehicles=25,
        ticks=3,
        seed=99,
        poison_count=0,
    )
    summary = FleetSimulator(isolated_config, bus, reg, sim_cfg).run()
    assert summary["virtual_fleet_size"] == 1_000_000
    assert summary["active_vehicles"] == 25
    assert summary["counts"]["telemetry_unique"] >= 50


@pytest.mark.unit
def test_murmur2_partition_stability_per_vehicle() -> None:
    p1 = murmur2_partition("VEH-000042", 12)
    p2 = murmur2_partition("VEH-000042", 12)
    assert p1 == p2
    assert 0 <= p1 < 12
    # Across 200 vehicles, all 12 partitions receive traffic
    spread = {murmur2_partition(f"VEH-{i:06d}", 12) for i in range(1, 201)}
    assert len(spread) == 12


@pytest.mark.unit
def test_geospatial_h3_geofences_and_coarsening() -> None:
    assert validate_gps(37.780, -122.410) is True
    assert validate_gps(0.0, 0.0) is False
    assert validate_gps(95.0, -122.0) is False

    cell6 = to_h3_cell(37.780, -122.410, resolution=6)
    cell9 = to_h3_cell(37.780, -122.410, resolution=9)
    assert cell6 is not None and cell9 is not None and cell6 != cell9

    fences = match_geofences("sf_bay", 37.785, -122.405)
    assert "sf_downtown_core" in fences

    # Point far outside downtown
    assert match_geofences("sf_bay", 37.710, -122.500) == []

    # Haversine distance SF downtown to Mission (~2-3 km)
    dist = haversine_km(37.7870, -122.4035, 37.7640, -122.4195)
    assert 2.0 < dist < 3.8

    # Route deviation near Market St route vs off-route
    near_m = route_deviation_meters("sf_bay", "sf_route_market", 37.7785, -122.4145)
    far_m = route_deviation_meters("sf_bay", "sf_route_market", 37.7785, -122.4545)
    assert near_m is not None and far_m is not None
    assert near_m < 50.0 and far_m > 2500.0

    # Privacy coarsening masks exact coordinates to cell centroid
    coarsened = coarsen_location(37.781234, -122.412345, resolution=6)
    assert coarsened["h3_cell"] == cell6
    assert abs(coarsened["coarsened_lat"] - 37.781234) > 1e-4
