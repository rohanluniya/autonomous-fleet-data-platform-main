"""Versioned, explainable anomaly detection rules & CEP patterns.

Shared identically by both the Apache Flink 2.1.3 PyFlink job and the native
Python stateful event-time stream engine. Every emitted anomaly carries
rule_id, rule_version, severity, event_time, detection_time, structured evidence,
and source_event_ids.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from fleet_platform.geospatial.spatial import to_h3_cell

_ANOMALY_NS = uuid.UUID("a1c2e3f4-5678-4abc-9def-0123456789ab")

RULE_CATALOG: dict[str, dict[str, Any]] = {
    "RULE_HARD_BRAKING": {
        "version": "2.0.0",
        "severity": "HIGH",
        "description": "Single-sample hard braking event (accel <= -5.5 m/s^2 and braking >= 80%).",
    },
    "RULE_REPEATED_HARD_BRAKING_CEP": {
        "version": "2.1.0",
        "severity": "CRITICAL",
        "description": "Complex event pattern: >= 3 hard-braking events within a 120s event-time window.",
    },
    "RULE_BATTERY_THERMAL_HIGH": {
        "version": "1.2.0",
        "severity": "CRITICAL",
        "description": "Battery temperature >= 60.0 C or >= 15 C jump above vehicle rolling baseline.",
    },
    "RULE_MOTOR_OVERHEAT": {
        "version": "1.1.0",
        "severity": "HIGH",
        "description": "Traction motor temperature >= 115.0 C.",
    },
    "RULE_SENSOR_DEGRADATION": {
        "version": "1.3.0",
        "severity": "HIGH",
        "description": "Composite autonomy perception sensor health score < 0.65.",
    },
    "RULE_LOW_BATTERY_DRIVING": {
        "version": "1.0.0",
        "severity": "MEDIUM",
        "description": "Vehicle actively driving with battery state of charge < 12%.",
    },
    "RULE_CONTEXTUAL_SPEED_SPIKE": {
        "version": "1.0.0",
        "severity": "MEDIUM",
        "description": "Speed exceeds vehicle rolling baseline mean by > 14 m/s after warmup.",
    },
}


@dataclass
class VehicleStreamingState:
    """Bounded per-vehicle keyed state maintained across event-time arrivals."""

    vehicle_id: str
    region: str = "unknown"
    software_version: str = "unknown"
    operational_state: str = "unknown"
    last_event_ts: float = 0.0
    last_battery_soc_pct: float = 0.0
    last_h3_res6: str | None = None
    sample_count: int = 0
    rolling_speed_mean: float = 0.0
    rolling_batt_temp_mean: float = 28.0
    # List of (event_ts_sec, event_id, braking_force_pct) within sliding CEP window
    hard_brake_window: list[tuple[float, str, float]] = field(default_factory=list)
    cep_last_fired_ts: float = -1.0
    gap_alerted: bool = False
    session_start_ts: float = 0.0
    session_event_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "vehicle_id": self.vehicle_id,
            "region": self.region,
            "software_version": self.software_version,
            "operational_state": self.operational_state,
            "last_event_ts": self.last_event_ts,
            "last_battery_soc_pct": self.last_battery_soc_pct,
            "last_h3_res6": self.last_h3_res6,
            "sample_count": self.sample_count,
            "rolling_speed_mean": self.rolling_speed_mean,
            "rolling_batt_temp_mean": self.rolling_batt_temp_mean,
            "hard_brake_window": [list(x) for x in self.hard_brake_window],
            "cep_last_fired_ts": self.cep_last_fired_ts,
            "gap_alerted": self.gap_alerted,
            "session_start_ts": self.session_start_ts,
            "session_event_count": self.session_event_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VehicleStreamingState:
        return cls(
            vehicle_id=data["vehicle_id"],
            region=data["region"],
            software_version=data["software_version"],
            operational_state=data["operational_state"],
            last_event_ts=float(data["last_event_ts"]),
            last_battery_soc_pct=float(data["last_battery_soc_pct"]),
            last_h3_res6=data.get("last_h3_res6"),
            sample_count=int(data["sample_count"]),
            rolling_speed_mean=float(data["rolling_speed_mean"]),
            rolling_batt_temp_mean=float(data["rolling_batt_temp_mean"]),
            hard_brake_window=[
                (float(x[0]), str(x[1]), float(x[2])) for x in data.get("hard_brake_window", [])
            ],
            cep_last_fired_ts=float(data.get("cep_last_fired_ts", -1.0)),
            gap_alerted=bool(data.get("gap_alerted", False)),
            session_start_ts=float(data.get("session_start_ts", 0.0)),
            session_event_count=int(data.get("session_event_count", 0)),
        )


def _make_anomaly(
    *,
    rule_id: str,
    event: dict[str, Any],
    evidence: dict[str, Any],
    source_event_ids: list[str],
    h3_cell: str | None,
) -> dict[str, Any]:
    meta = RULE_CATALOG[rule_id]
    pl = event["payload"]
    loc = event.get("location") or {}
    # Deterministic anomaly_id prevents duplicate downstream alerts upon checkpoint replay
    key_seed = (
        f"{rule_id}:{meta['version']}:{event['vehicle_id']}:{','.join(sorted(source_event_ids))}"
    )
    return {
        "anomaly_id": str(uuid.uuid5(_ANOMALY_NS, key_seed)),
        "vehicle_id": event["vehicle_id"],
        "trip_id": event.get("trip_id"),
        "region": pl["region"],
        "software_version": pl["software_version"],
        "rule_id": rule_id,
        "rule_version": meta["version"],
        "severity": meta["severity"],
        "event_time": event["event_time"],
        "detection_time": datetime.now(UTC).isoformat(),
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "h3_cell_res6": h3_cell,
        "evidence_json": json.dumps(evidence, sort_keys=True),
        "source_event_ids": source_event_ids,
        "correlation_id": event["correlation_id"],
        "trace_id": event["trace_id"],
        "schema_version": "1.0.0",
    }


def evaluate_telemetry_rules(
    event: dict[str, Any],
    state: VehicleStreamingState,
    *,
    event_ts_sec: float,
    cep_window_seconds: float = 120.0,
    cep_min_count: int = 3,
) -> list[dict[str, Any]]:
    """Evaluate all deterministic, contextual-baseline, and CEP rules against one telemetry event."""
    pl = event["payload"]
    loc = event.get("location") or {}
    lat = loc.get("latitude")
    lon = loc.get("longitude")
    h3_cell = to_h3_cell(lat, lon, resolution=6) if lat is not None and lon is not None else None

    speed = float(pl["speed_mps"])
    accel = float(pl["acceleration_mps2"])
    braking = float(pl["braking_force_pct"])
    batt_soc = float(pl["battery_soc_pct"])
    batt_temp = float(pl["battery_temp_c"])
    motor_temp = float(pl["motor_temp_c"])
    sensor_health = float(pl["sensor_health_score"])
    vstate = str(pl["vehicle_state"])
    ev_id = str(event["event_id"])

    anomalies: list[dict[str, Any]] = []

    # 1. Single-event Hard Braking + Sliding-Window CEP Repeated Hard Braking
    if accel <= -5.5 and braking >= 80.0 and speed >= 1.5:
        anomalies.append(
            _make_anomaly(
                rule_id="RULE_HARD_BRAKING",
                event=event,
                evidence={
                    "acceleration_mps2": accel,
                    "braking_force_pct": braking,
                    "speed_mps": speed,
                    "threshold_accel_mps2": -5.5,
                    "threshold_braking_pct": 80.0,
                },
                source_event_ids=[ev_id],
                h3_cell=h3_cell,
            )
        )
        # Update sliding event-time window for CEP pattern
        state.hard_brake_window.append((event_ts_sec, ev_id, braking))
        cutoff = event_ts_sec - cep_window_seconds
        state.hard_brake_window = [item for item in state.hard_brake_window if item[0] >= cutoff]
        if (
            len(state.hard_brake_window) >= cep_min_count
            and (event_ts_sec - state.cep_last_fired_ts) >= 15.0
        ):
            src_ids = [item[1] for item in state.hard_brake_window]
            span_s = round(
                max(item[0] for item in state.hard_brake_window)
                - min(item[0] for item in state.hard_brake_window),
                2,
            )
            anomalies.append(
                _make_anomaly(
                    rule_id="RULE_REPEATED_HARD_BRAKING_CEP",
                    event=event,
                    evidence={
                        "hard_brakes_in_window": len(state.hard_brake_window),
                        "window_seconds": cep_window_seconds,
                        "observed_span_seconds": span_s,
                        "peak_braking_force_pct": max(item[2] for item in state.hard_brake_window),
                    },
                    source_event_ids=src_ids,
                    h3_cell=h3_cell,
                )
            )
            state.cep_last_fired_ts = event_ts_sec

    # 2. Battery Thermal High (Deterministic threshold + Contextual baseline jump)
    temp_delta = round(batt_temp - state.rolling_batt_temp_mean, 2)
    if batt_temp >= 60.0 or (state.sample_count >= 3 and temp_delta >= 15.0):
        anomalies.append(
            _make_anomaly(
                rule_id="RULE_BATTERY_THERMAL_HIGH",
                event=event,
                evidence={
                    "battery_temp_c": batt_temp,
                    "rolling_baseline_temp_c": round(state.rolling_batt_temp_mean, 2),
                    "delta_above_baseline_c": temp_delta,
                    "threshold_c": 60.0,
                },
                source_event_ids=[ev_id],
                h3_cell=h3_cell,
            )
        )

    # 3. Motor Overheat
    if motor_temp >= 115.0:
        anomalies.append(
            _make_anomaly(
                rule_id="RULE_MOTOR_OVERHEAT",
                event=event,
                evidence={"motor_temp_c": motor_temp, "threshold_c": 115.0},
                source_event_ids=[ev_id],
                h3_cell=h3_cell,
            )
        )

    # 4. Sensor Health Degradation
    if sensor_health < 0.65:
        anomalies.append(
            _make_anomaly(
                rule_id="RULE_SENSOR_DEGRADATION",
                event=event,
                evidence={
                    "sensor_health_score": sensor_health,
                    "threshold": 0.65,
                    "lidar_status": pl.get("lidar_status", "UNKNOWN"),
                },
                source_event_ids=[ev_id],
                h3_cell=h3_cell,
            )
        )

    # 5. Low Battery While Actively Driving
    if vstate == "driving" and batt_soc < 12.0:
        anomalies.append(
            _make_anomaly(
                rule_id="RULE_LOW_BATTERY_DRIVING",
                event=event,
                evidence={"battery_soc_pct": batt_soc, "threshold_pct": 12.0},
                source_event_ids=[ev_id],
                h3_cell=h3_cell,
            )
        )

    # 6. Contextual Speed Spike above rolling baseline
    if state.sample_count >= 4 and (speed - state.rolling_speed_mean) > 14.0:
        anomalies.append(
            _make_anomaly(
                rule_id="RULE_CONTEXTUAL_SPEED_SPIKE",
                event=event,
                evidence={
                    "speed_mps": speed,
                    "rolling_baseline_speed_mps": round(state.rolling_speed_mean, 2),
                    "delta_mps": round(speed - state.rolling_speed_mean, 2),
                },
                source_event_ids=[ev_id],
                h3_cell=h3_cell,
            )
        )

    # Update exponential moving baselines & vehicle state AFTER evaluating rules
    alpha = 0.25 if state.sample_count > 0 else 1.0
    state.rolling_speed_mean = (
        speed if state.sample_count == 0 else (1 - alpha) * state.rolling_speed_mean + alpha * speed
    )
    state.rolling_batt_temp_mean = (
        batt_temp
        if state.sample_count == 0
        else (1 - alpha) * state.rolling_batt_temp_mean + alpha * batt_temp
    )
    state.sample_count += 1
    state.region = str(pl["region"])
    state.software_version = str(pl["software_version"])
    state.operational_state = vstate
    state.last_battery_soc_pct = batt_soc
    state.last_h3_res6 = h3_cell
    if event_ts_sec > state.last_event_ts:
        state.last_event_ts = event_ts_sec
    state.gap_alerted = False

    return anomalies
