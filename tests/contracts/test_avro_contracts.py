"""Avro contract tests covering wire framing, schema compatibility (backward & forward),
and breaking-change rejection.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from fleet_platform.contracts.registry import AvroContractRegistry, SchemaValidationError


def _sample_v1_telemetry() -> dict:
    return {
        "event_id": "evt-001",
        "vehicle_id": "VEH-000001",
        "trip_id": "TRIP-001",
        "event_type": "telemetry",
        "event_time": "2026-11-24T12:00:00+00:00",
        "ingest_time": "2026-11-24T12:00:01+00:00",
        "sequence_number": 1,
        "schema_version": "1.0.0",
        "source_id": "veh-gateway-0",
        "correlation_id": "corr-1",
        "trace_id": "trace-1",
        "location": {
            "latitude": 37.7749,
            "longitude": -122.4194,
            "heading_deg": 90.0,
            "altitude_m": 12.0,
        },
        "payload": {
            "vehicle_state": "driving",
            "region": "sf_bay",
            "vehicle_model": "AV-Sedan-G3",
            "software_version": "v4.2.0",
            "speed_mps": 14.5,
            "acceleration_mps2": 0.4,
            "braking_force_pct": 0.0,
            "battery_soc_pct": 81.2,
            "battery_temp_c": 29.5,
            "motor_temp_c": 54.0,
            "odometer_km": 12042.5,
            "fault_codes": [],
            "sensor_health_score": 0.99,
            "lidar_status": "OK",
            "camera_status": "OK",
            "radar_status": "OK",
        },
        "metadata": {"synthetic": "true"},
    }


@pytest.mark.contract
def test_avro_roundtrip_and_poison_rejection() -> None:
    reg = AvroContractRegistry()
    rec = _sample_v1_telemetry()
    encoded = reg.serialize("fleet.telemetry.raw", rec)
    schema_id, decoded = reg.deserialize(encoded, expected_topic="fleet.telemetry.raw")
    assert schema_id == 1
    assert decoded["event_id"] == "evt-001"
    assert decoded["payload"]["speed_mps"] == 14.5

    # Corrupt wire header rejected
    with pytest.raises(SchemaValidationError):
        reg.deserialize(b"\xffcorrupt", expected_topic="fleet.telemetry.raw")

    # Wrong expected topic rejected
    with pytest.raises(SchemaValidationError):
        reg.deserialize(encoded, expected_topic="fleet.trips.raw")


@pytest.mark.contract
def test_avro_v1_to_v2_backward_and_forward_compatibility() -> None:
    reg = AvroContractRegistry()
    v1_schema = reg.get_raw_schema("fleet.telemetry.raw")
    v2_path = (
        Path(__file__).resolve().parents[2] / "schemas" / "avro" / "v2" / "telemetry_event.avsc"
    )
    v2_schema = json.loads(v2_path.read_text(encoding="utf-8"))

    # Backward compatibility: v2 reader reads v1 writer payload
    ok_back, msg_back = reg.check_compatibility(v1_schema, v2_schema, _sample_v1_telemetry())
    assert ok_back, f"Expected backward compatibility: {msg_back}"

    # Forward compatibility: v1 reader reads v2 writer payload (ignoring new optional fields)
    v2_sample = copy.deepcopy(_sample_v1_telemetry())
    v2_sample["schema_version"] = "2.0.0"
    v2_sample["payload"]["ambient_temp_c"] = 21.5
    v2_sample["payload"]["tire_pressure_psi"] = 36.2
    ok_fwd, msg_fwd = reg.check_compatibility(v2_schema, v1_schema, v2_sample)
    assert ok_fwd, f"Expected forward compatibility: {msg_fwd}"


@pytest.mark.contract
def test_breaking_schema_change_detected() -> None:
    reg = AvroContractRegistry()
    v1_schema = reg.get_raw_schema("fleet.telemetry.raw")
    breaking = copy.deepcopy(v1_schema)
    # Adding a required non-nullable field without default breaks backward compatibility
    breaking["fields"].append({"name": "mandatory_new_field", "type": "string"})
    ok, _ = reg.check_compatibility(v1_schema, breaking, _sample_v1_telemetry())
    assert ok is False
