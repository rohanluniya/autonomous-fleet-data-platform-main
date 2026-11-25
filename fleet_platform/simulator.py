"""Deterministic, configurable synthetic autonomous vehicle fleet simulator.

Explicitly distinguishes virtual fleet population (up to 1,000,000+ identities)
from concurrently active emitting vehicles. Produces Avro-encoded events across
telemetry, GPS, diagnostics, trips, and explicitly labeled synthetic autonomy topics.
"""

from __future__ import annotations

import json
import math
import random
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.geospatial.spatial import METRO_REGIONS, haversine_km
from fleet_platform.observability import get_logger, get_metrics, new_trace_context
from fleet_platform.transport.base import EventBus

_NAMESPACE_FLEET = uuid.UUID("7f3b5c4a-9e21-4d8a-a7c3-1b2c3d4e5f60")

VEHICLE_MODELS = ("AV-Sedan-G3", "AV-Shuttle-G4", "AV-Crossover-G4")
SOFTWARE_VERSIONS = ("v4.1.0", "v4.2.0", "v4.3.0-rc1")
VEHICLE_STATES = ("driving", "parked", "charging", "offline", "maintenance")
DISENGAGEMENT_REASONS = (
    "SIMULATED_CONSTRUCTION_CONE_AMBIGUITY",
    "SIMULATED_EMERGENCY_VEHICLE_YIELD",
    "SIMULATED_SENSOR_OCCLUSION_RAIN",
    "SIMULATED_UNMAPPED_DETOUR",
)

WORKLOAD_PROFILES: dict[str, dict[str, Any]] = {
    "smoke": {
        "virtual_fleet_size": 100,
        "active_vehicles": 100,
        "ticks": 8,
        "step_seconds": 5,
        "duplicate_rate": 0.03,
        "out_of_order_rate": 0.04,
        "anomaly_rate": 0.02,
        "poison_count": 3,
    },
    "local": {
        "virtual_fleet_size": 1_000,
        "active_vehicles": 1_000,
        "ticks": 6,
        "step_seconds": 10,
        "duplicate_rate": 0.02,
        "out_of_order_rate": 0.03,
        "anomaly_rate": 0.015,
        "poison_count": 5,
    },
    "stress": {
        "virtual_fleet_size": 10_000,
        "active_vehicles": 10_000,
        "ticks": 3,
        "step_seconds": 10,
        "duplicate_rate": 0.01,
        "out_of_order_rate": 0.02,
        "anomaly_rate": 0.01,
        "poison_count": 10,
    },
    "virtual-population": {
        "virtual_fleet_size": 1_000_000,
        "active_vehicles": 2_000,
        "ticks": 4,
        "step_seconds": 10,
        "duplicate_rate": 0.015,
        "out_of_order_rate": 0.02,
        "anomaly_rate": 0.012,
        "poison_count": 5,
    },
}


@dataclass
class SimulationConfig:
    virtual_fleet_size: int = 100
    active_vehicles: int = 100
    ticks: int = 8
    step_seconds: int = 10
    seed: int = 42
    start_time_utc: datetime = field(
        default_factory=lambda: datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)
    )
    duplicate_rate: float = 0.02
    out_of_order_rate: float = 0.03
    clock_skew_rate: float = 0.01
    connectivity_gap_rate: float = 0.015
    anomaly_rate: float = 0.015
    poison_count: int = 3
    target_events_per_sec: float | None = None
    emit_auxiliary_topics: bool = True
    inject_known_hard_brake_vehicle: str = "VEH-000001"
    ground_truth_output: Path | None = None

    @classmethod
    def from_profile(cls, profile_name: str, **overrides: Any) -> SimulationConfig:
        if profile_name not in WORKLOAD_PROFILES:
            raise ValueError(
                f"Unknown profile '{profile_name}'. Choose from {sorted(WORKLOAD_PROFILES)}"
            )
        params = dict(WORKLOAD_PROFILES[profile_name])
        params.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**params)


@dataclass
class _VehicleAgent:
    vehicle_id: str
    region_id: str
    route_id: str
    vehicle_model: str
    software_version: str
    state: str
    route_progress: float
    speed_mps: float
    battery_soc_pct: float
    battery_temp_c: float
    motor_temp_c: float
    odometer_km: float
    sensor_health_score: float
    sequence_number: int = 0
    trip_id: str | None = None
    offline_ticks_remaining: int = 0
    buffered_events: list[dict[str, Any]] = field(default_factory=list)


def deterministic_event_id(seed: int, vehicle_id: str, event_type: str, seq: int) -> str:
    return str(uuid.uuid5(_NAMESPACE_FLEET, f"{seed}:{vehicle_id}:{event_type}:{seq}"))


def _interpolate_route(
    waypoints: list[tuple[float, float]], progress: float
) -> tuple[float, float, float]:
    """Return (lat, lon, heading_deg) along route waypoints for progress in [0, 1]."""
    p = max(0.0, min(1.0, progress))
    n_seg = len(waypoints) - 1
    scaled = p * n_seg
    idx = min(int(scaled), n_seg - 1)
    frac = scaled - idx
    lat1, lon1 = waypoints[idx]
    lat2, lon2 = waypoints[idx + 1]
    lat = lat1 + (lat2 - lat1) * frac
    lon = lon1 + (lon2 - lon1) * frac
    heading = (math.degrees(math.atan2(lon2 - lon1, lat2 - lat1)) + 360.0) % 360.0
    return lat, lon, heading


class FleetSimulator:
    """Deterministic synthetic fleet generator with fault and ground-truth anomaly injection."""

    def __init__(
        self,
        platform_config: PlatformConfig,
        bus: EventBus,
        registry: AvroContractRegistry,
        sim_config: SimulationConfig,
    ) -> None:
        self.platform_config = platform_config
        self.bus = bus
        self.registry = registry
        self.sim_config = sim_config
        self.rng = random.Random(sim_config.seed)
        self.logger = get_logger("fleet-simulator", "generator", environment=platform_config.env)
        self.metrics = get_metrics()
        self._shutdown_requested = False

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        self._shutdown_requested = True
        self.logger.warning(
            "simulator.shutdown_signal",
            f"Received signal {signum}; draining current batch cleanly",
        )

    def _init_active_agents(self) -> list[_VehicleAgent]:
        cfg = self.sim_config
        if cfg.active_vehicles > cfg.virtual_fleet_size:
            raise ValueError("active_vehicles cannot exceed virtual_fleet_size")

        region_ids = list(METRO_REGIONS.keys())
        # Deterministic selection of active vehicle indices out of virtual_fleet_size
        if cfg.active_vehicles == cfg.virtual_fleet_size:
            indices = list(range(1, cfg.active_vehicles + 1))
        else:
            # Always include vehicle #1 for deterministic known anomaly testing
            pool = list(range(2, cfg.virtual_fleet_size + 1))
            sampled = sorted(self.rng.sample(pool, cfg.active_vehicles - 1))
            indices = [1, *sampled]

        agents: list[_VehicleAgent] = []
        for idx in indices:
            vid = f"VEH-{idx:06d}"
            region_id = region_ids[idx % len(region_ids)]
            routes = list(METRO_REGIONS[region_id].routes.keys())
            route_id = routes[(idx // len(region_ids)) % len(routes)]
            # Weighted realistic states: 78% driving, 10% parked, 7% charging, 3% offline, 2% maintenance
            r = self.rng.random()
            if vid == cfg.inject_known_hard_brake_vehicle or r < 0.78:
                state = "driving"
            elif r < 0.88:
                state = "parked"
            elif r < 0.95:
                state = "charging"
            elif r < 0.98:
                state = "offline"
            else:
                state = "maintenance"

            agents.append(
                _VehicleAgent(
                    vehicle_id=vid,
                    region_id=region_id,
                    route_id=route_id,
                    vehicle_model=VEHICLE_MODELS[(idx // 3) % len(VEHICLE_MODELS)],
                    software_version=SOFTWARE_VERSIONS[(idx // 9) % len(SOFTWARE_VERSIONS)],
                    state=state,
                    route_progress=self.rng.uniform(0.02, 0.40),
                    speed_mps=self.rng.uniform(8.0, 18.0) if state == "driving" else 0.0,
                    battery_soc_pct=self.rng.uniform(35.0, 96.0),
                    battery_temp_c=self.rng.uniform(24.0, 36.0),
                    motor_temp_c=self.rng.uniform(42.0, 68.0),
                    odometer_km=round(self.rng.uniform(2_500.0, 85_000.0), 2),
                    sensor_health_score=round(self.rng.uniform(0.92, 1.0), 3),
                )
            )
        return agents

    def run(self) -> dict[str, Any]:
        """Run deterministic simulation and return comprehensive summary & ground truth."""
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        except ValueError:
            # Running inside non-main thread (e.g. pytest worker)
            pass

        t0 = time.perf_counter()
        cfg = self.sim_config
        trace_id, corr_id = new_trace_context()
        pipeline_run_id = f"sim-{cfg.seed}-{int(time.time())}"
        self.bus.ensure_topics()
        agents = self._init_active_agents()

        ground_truth_anomalies: list[dict[str, Any]] = []
        counts = {
            "telemetry_sent": 0,
            "telemetry_unique": 0,
            "duplicates_injected": 0,
            "out_of_order_injected": 0,
            "late_buffered_flushed": 0,
            "trips_sent": 0,
            "autonomy_sent": 0,
            "diagnostics_sent": 0,
            "gps_sent": 0,
            "poison_injected": 0,
            "bytes_produced": 0,
        }

        # Pre-start trips for driving vehicles at tick 0
        trip_batch: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []
        for agent in agents:
            if agent.state == "driving":
                agent.trip_id = f"TRIP-{cfg.seed}-{agent.vehicle_id}-001"
                lat, lon, _ = _interpolate_route(
                    METRO_REGIONS[agent.region_id].routes[agent.route_id],
                    agent.route_progress,
                )
                agent.sequence_number += 1
                ev_time = cfg.start_time_utc
                trip_rec = {
                    "event_id": deterministic_event_id(
                        cfg.seed, agent.vehicle_id, "trip_start", agent.sequence_number
                    ),
                    "vehicle_id": agent.vehicle_id,
                    "trip_id": agent.trip_id,
                    "event_type": "trip_start",
                    "event_time": ev_time.isoformat(),
                    "ingest_time": datetime.now(UTC).isoformat(),
                    "sequence_number": agent.sequence_number,
                    "schema_version": "1.0.0",
                    "source_id": f"sim-node-{hash(agent.vehicle_id) % 4}",
                    "correlation_id": corr_id,
                    "trace_id": trace_id,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "region": agent.region_id,
                    "software_version": agent.software_version,
                    "odometer_km": round(agent.odometer_km, 3),
                    "battery_soc_pct": round(agent.battery_soc_pct, 2),
                    "route_id": agent.route_id,
                    "metadata": {"synthetic": "true", "sim_seed": str(cfg.seed)},
                }
                raw_bytes = self.registry.serialize("fleet.trips.raw", trip_rec)
                trip_batch.append(
                    (agent.vehicle_id, raw_bytes, int(ev_time.timestamp() * 1000), None)
                )
                counts["trips_sent"] += 1
                counts["bytes_produced"] += len(raw_bytes)
        if trip_batch:
            self.bus.produce_batch("fleet.trips.raw", trip_batch)

        delayed_queue: list[tuple[int, str, dict[str, Any]]] = []

        for tick in range(cfg.ticks):
            if self._shutdown_requested:
                break
            tick_start = time.perf_counter()
            tick_time = cfg.start_time_utc + timedelta(seconds=tick * cfg.step_seconds)
            telemetry_batch: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []
            gps_batch: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []
            diag_batch: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []
            autonomy_batch: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []

            # Flush any buffered out-of-order events scheduled for this tick
            remaining_delayed: list[tuple[int, str, dict[str, Any]]] = []
            for release_tick, vid, delayed_rec in delayed_queue:
                if release_tick <= tick:
                    delayed_rec["ingest_time"] = (
                        tick_time + timedelta(milliseconds=self.rng.randint(80, 450))
                    ).isoformat()
                    delayed_rec["metadata"]["delayed_flush"] = "true"
                    raw = self.registry.serialize("fleet.telemetry.raw", delayed_rec)
                    telemetry_batch.append(
                        (
                            vid,
                            raw,
                            int(
                                datetime.fromisoformat(delayed_rec["event_time"]).timestamp() * 1000
                            ),
                            {"delayed_flush": "true"},
                        )
                    )
                    counts["telemetry_sent"] += 1
                    counts["late_buffered_flushed"] += 1
                    counts["bytes_produced"] += len(raw)
                else:
                    remaining_delayed.append((release_tick, vid, delayed_rec))
            delayed_queue = remaining_delayed

            for agent in agents:
                # Connectivity loss simulation
                if agent.offline_ticks_remaining > 0:
                    agent.offline_ticks_remaining -= 1
                    continue

                if (
                    agent.vehicle_id != cfg.inject_known_hard_brake_vehicle
                    and agent.state == "driving"
                    and self.rng.random() < cfg.connectivity_gap_rate
                    and tick < cfg.ticks - 2
                ):
                    agent.offline_ticks_remaining = 2
                    continue

                # Kinematics & state progression
                waypoints = METRO_REGIONS[agent.region_id].routes[agent.route_id]
                prev_lat, prev_lon, _ = _interpolate_route(waypoints, agent.route_progress)
                if agent.state == "driving":
                    accel = self.rng.uniform(-1.5, 1.8)
                    new_speed = max(0.5, min(28.0, agent.speed_mps + accel * 1.5))
                    braking_pct = max(0.0, min(40.0, -accel * 15.0)) if accel < 0 else 0.0
                    agent.speed_mps = new_speed
                    step_km = (new_speed * cfg.step_seconds) / 1000.0
                    agent.route_progress = (agent.route_progress + step_km * 0.12) % 0.98
                    agent.odometer_km += step_km
                    agent.battery_soc_pct = max(5.0, agent.battery_soc_pct - step_km * 0.35)
                    agent.battery_temp_c = min(
                        58.0, agent.battery_temp_c + self.rng.uniform(-0.3, 0.5)
                    )
                    agent.motor_temp_c = min(
                        105.0, agent.motor_temp_c + self.rng.uniform(-0.8, 1.1)
                    )
                elif agent.state == "charging":
                    accel = 0.0
                    braking_pct = 0.0
                    agent.speed_mps = 0.0
                    agent.battery_soc_pct = min(100.0, agent.battery_soc_pct + 0.8)
                    agent.battery_temp_c = max(22.0, agent.battery_temp_c + 0.2)
                    agent.motor_temp_c = max(25.0, agent.motor_temp_c - 0.5)
                else:
                    accel = 0.0
                    braking_pct = 0.0
                    agent.speed_mps = 0.0
                    agent.battery_temp_c = max(20.0, agent.battery_temp_c - 0.2)
                    agent.motor_temp_c = max(22.0, agent.motor_temp_c - 0.4)

                lat, lon, heading = _interpolate_route(waypoints, agent.route_progress)
                # Slight lateral jitter (~10m) for realism
                lat += self.rng.uniform(-0.0001, 0.0001)
                lon += self.rng.uniform(-0.0001, 0.0001)
                step_geo_km = haversine_km(prev_lat, prev_lon, lat, lon)
                if agent.state == "driving" and step_geo_km > 0:
                    agent.odometer_km += 0.0  # already incremented by kinematic step

                fault_codes: list[str] = []
                injected_label: str | None = None

                # Controlled deterministic hard-braking burst on known vehicle (for Acceptance Test E)
                if agent.vehicle_id == cfg.inject_known_hard_brake_vehicle and tick in (1, 2, 3):
                    accel = -7.8
                    braking_pct = 92.5
                    agent.speed_mps = max(2.0, agent.speed_mps)
                    injected_label = "HARD_BRAKING_BURST"
                elif agent.state == "driving" and self.rng.random() < cfg.anomaly_rate:
                    kind = self.rng.choice(
                        ["HARD_BRAKING", "BATTERY_OVERHEAT", "MOTOR_OVERHEAT", "SENSOR_DEGRADATION"]
                    )
                    injected_label = kind
                    if kind == "HARD_BRAKING":
                        accel = self.rng.uniform(-8.5, -5.8)
                        braking_pct = self.rng.uniform(82.0, 98.0)
                    elif kind == "BATTERY_OVERHEAT":
                        agent.battery_temp_c = self.rng.uniform(63.0, 74.0)
                        fault_codes.append("DTC_BATT_THERMAL_HIGH")
                    elif kind == "MOTOR_OVERHEAT":
                        agent.motor_temp_c = self.rng.uniform(116.0, 132.0)
                        fault_codes.append("DTC_MOTOR_OVERHEAT")
                    elif kind == "SENSOR_DEGRADATION":
                        agent.sensor_health_score = round(self.rng.uniform(0.32, 0.58), 3)
                        fault_codes.append("DTC_LIDAR_DEGRADED")

                # Event timestamp (with occasional synthetic clock skew)
                ev_time = tick_time
                skew_applied = 0
                if (
                    agent.vehicle_id != cfg.inject_known_hard_brake_vehicle
                    and self.rng.random() < cfg.clock_skew_rate
                ):
                    skew_applied = self.rng.choice([-45, -20, 3])
                    ev_time = tick_time + timedelta(seconds=skew_applied)

                agent.sequence_number += 1
                event_id = deterministic_event_id(
                    cfg.seed, agent.vehicle_id, "telemetry", agent.sequence_number
                )

                rec: dict[str, Any] = {
                    "event_id": event_id,
                    "vehicle_id": agent.vehicle_id,
                    "trip_id": agent.trip_id,
                    "event_type": "telemetry",
                    "event_time": ev_time.isoformat(),
                    "ingest_time": (
                        tick_time + timedelta(milliseconds=self.rng.randint(45, 320))
                    ).isoformat(),
                    "sequence_number": agent.sequence_number,
                    "schema_version": "1.0.0",
                    "source_id": f"veh-gateway-{hash(agent.vehicle_id) % 8}",
                    "correlation_id": corr_id,
                    "trace_id": trace_id,
                    "location": {
                        "latitude": round(lat, 6),
                        "longitude": round(lon, 6),
                        "heading_deg": round(heading, 2),
                        "altitude_m": 15.0,
                    },
                    "payload": {
                        "vehicle_state": agent.state,
                        "region": agent.region_id,
                        "vehicle_model": agent.vehicle_model,
                        "software_version": agent.software_version,
                        "speed_mps": round(agent.speed_mps, 3),
                        "acceleration_mps2": round(accel, 3),
                        "braking_force_pct": round(braking_pct, 2),
                        "battery_soc_pct": round(agent.battery_soc_pct, 2),
                        "battery_temp_c": round(agent.battery_temp_c, 2),
                        "motor_temp_c": round(agent.motor_temp_c, 2),
                        "odometer_km": round(agent.odometer_km, 3),
                        "fault_codes": fault_codes,
                        "sensor_health_score": round(agent.sensor_health_score, 3),
                        "lidar_status": "DEGRADED" if agent.sensor_health_score < 0.65 else "OK",
                        "camera_status": "OK",
                        "radar_status": "OK",
                    },
                    "metadata": {
                        "synthetic": "true",
                        "sim_seed": str(cfg.seed),
                        "route_id": agent.route_id,
                        "clock_skew_s": str(skew_applied),
                    },
                }

                if injected_label:
                    ground_truth_anomalies.append(
                        {
                            "event_id": event_id,
                            "vehicle_id": agent.vehicle_id,
                            "trip_id": agent.trip_id,
                            "event_time": ev_time.isoformat(),
                            "injected_type": injected_label,
                            "region": agent.region_id,
                            "software_version": agent.software_version,
                        }
                    )

                counts["telemetry_unique"] += 1

                # Out-of-order delayed buffering simulation
                if (
                    agent.vehicle_id != cfg.inject_known_hard_brake_vehicle
                    and tick < cfg.ticks - 2
                    and self.rng.random() < cfg.out_of_order_rate
                ):
                    delayed_queue.append((tick + 2, agent.vehicle_id, rec))
                    counts["out_of_order_injected"] += 1
                    continue

                raw_bytes = self.registry.serialize("fleet.telemetry.raw", rec)
                ts_ms = int(ev_time.timestamp() * 1000)
                telemetry_batch.append((agent.vehicle_id, raw_bytes, ts_ms, None))
                counts["telemetry_sent"] += 1
                counts["bytes_produced"] += len(raw_bytes)

                # Controlled duplicate delivery injection (exact same event_id)
                if self.rng.random() < cfg.duplicate_rate:
                    telemetry_batch.append(
                        (agent.vehicle_id, raw_bytes, ts_ms, {"injected_duplicate": "true"})
                    )
                    counts["telemetry_sent"] += 1
                    counts["duplicates_injected"] += 1
                    counts["bytes_produced"] += len(raw_bytes)

                # Auxiliary domain topics
                if cfg.emit_auxiliary_topics:
                    if tick % 2 == 0 and agent.state == "driving":
                        gps_rec = {
                            "event_id": deterministic_event_id(
                                cfg.seed, agent.vehicle_id, "gps", agent.sequence_number
                            ),
                            "vehicle_id": agent.vehicle_id,
                            "trip_id": agent.trip_id,
                            "event_type": "gps",
                            "event_time": ev_time.isoformat(),
                            "ingest_time": datetime.now(UTC).isoformat(),
                            "sequence_number": agent.sequence_number,
                            "schema_version": "1.0.0",
                            "source_id": f"gps-{agent.region_id}",
                            "correlation_id": corr_id,
                            "trace_id": trace_id,
                            "latitude": round(lat, 6),
                            "longitude": round(lon, 6),
                            "heading_deg": round(heading, 2),
                            "speed_mps": round(agent.speed_mps, 3),
                            "region": agent.region_id,
                            "metadata": {"synthetic": "true"},
                        }
                        gb = self.registry.serialize("fleet.gps.raw", gps_rec)
                        gps_batch.append((agent.vehicle_id, gb, ts_ms, None))
                        counts["gps_sent"] += 1
                        counts["bytes_produced"] += len(gb)

                    if fault_codes:
                        diag_rec = {
                            "event_id": deterministic_event_id(
                                cfg.seed, agent.vehicle_id, "diagnostics", agent.sequence_number
                            ),
                            "vehicle_id": agent.vehicle_id,
                            "trip_id": agent.trip_id,
                            "event_type": "diagnostics",
                            "event_time": ev_time.isoformat(),
                            "ingest_time": datetime.now(UTC).isoformat(),
                            "sequence_number": agent.sequence_number,
                            "schema_version": "1.0.0",
                            "source_id": "veh-bms-diag",
                            "correlation_id": corr_id,
                            "trace_id": trace_id,
                            "region": agent.region_id,
                            "software_version": agent.software_version,
                            "fault_codes": fault_codes,
                            "battery_temp_c": round(agent.battery_temp_c, 2),
                            "motor_temp_c": round(agent.motor_temp_c, 2),
                            "sensor_health_score": round(agent.sensor_health_score, 3),
                            "severity_hint": "CRITICAL" if agent.battery_temp_c > 62 else "WARNING",
                            "metadata": {"synthetic": "true"},
                        }
                        db = self.registry.serialize("fleet.diagnostics.raw", diag_rec)
                        diag_batch.append((agent.vehicle_id, db, ts_ms, None))
                        counts["diagnostics_sent"] += 1
                        counts["bytes_produced"] += len(db)

                    if agent.state == "driving" and (
                        (agent.vehicle_id == cfg.inject_known_hard_brake_vehicle and tick == 2)
                        or self.rng.random() < 0.008
                    ):
                        auto_rec = {
                            "event_id": deterministic_event_id(
                                cfg.seed, agent.vehicle_id, "autonomy", agent.sequence_number
                            ),
                            "vehicle_id": agent.vehicle_id,
                            "trip_id": agent.trip_id,
                            "event_type": "synthetic_autonomy_disengagement",
                            "event_time": ev_time.isoformat(),
                            "ingest_time": datetime.now(UTC).isoformat(),
                            "sequence_number": agent.sequence_number,
                            "schema_version": "1.0.0",
                            "source_id": "sim-autonomy-stack",
                            "correlation_id": corr_id,
                            "trace_id": trace_id,
                            "latitude": round(lat, 6),
                            "longitude": round(lon, 6),
                            "region": agent.region_id,
                            "software_version": agent.software_version,
                            "disengagement_reason": self.rng.choice(DISENGAGEMENT_REASONS),
                            "autonomy_mode_before": "SIMULATED_L4_URBAN",
                            "takeover_latency_ms": self.rng.randint(250, 1450),
                            "synthetic_flag": True,
                            "metadata": {
                                "synthetic": "true",
                                "disclaimer": "synthetic_simulation_not_real_fsd",
                            },
                        }
                        ab = self.registry.serialize("fleet.autonomy.raw", auto_rec)
                        autonomy_batch.append((agent.vehicle_id, ab, ts_ms, None))
                        counts["autonomy_sent"] += 1
                        counts["bytes_produced"] += len(ab)

            if telemetry_batch:
                self.bus.produce_batch("fleet.telemetry.raw", telemetry_batch)
                self.metrics.events_produced_total.labels(
                    topic="fleet.telemetry.raw", event_type="telemetry"
                ).inc(len(telemetry_batch))
            if gps_batch:
                self.bus.produce_batch("fleet.gps.raw", gps_batch)
            if diag_batch:
                self.bus.produce_batch("fleet.diagnostics.raw", diag_batch)
            if autonomy_batch:
                self.bus.produce_batch("fleet.autonomy.raw", autonomy_batch)

            # Optional rate limiting
            if cfg.target_events_per_sec and telemetry_batch:
                target_dur = len(telemetry_batch) / cfg.target_events_per_sec
                elapsed_tick = time.perf_counter() - tick_start
                if target_dur > elapsed_tick:
                    time.sleep(target_dur - elapsed_tick)

        # Flush any remaining delayed events at end of simulation
        end_time = cfg.start_time_utc + timedelta(seconds=cfg.ticks * cfg.step_seconds)
        if delayed_queue:
            tail_batch = []
            for _, vid, delayed_rec in delayed_queue:
                delayed_rec["ingest_time"] = (
                    end_time + timedelta(milliseconds=self.rng.randint(100, 500))
                ).isoformat()
                delayed_rec["metadata"]["delayed_flush"] = "true"
                raw = self.registry.serialize("fleet.telemetry.raw", delayed_rec)
                tail_batch.append(
                    (
                        vid,
                        raw,
                        int(datetime.fromisoformat(delayed_rec["event_time"]).timestamp() * 1000),
                        {"delayed_flush": "true"},
                    )
                )
                counts["telemetry_sent"] += 1
                counts["late_buffered_flushed"] += 1
                counts["bytes_produced"] += len(raw)
            self.bus.produce_batch("fleet.telemetry.raw", tail_batch)

        # Emit trip_end events for active driving vehicles
        trip_end_batch = []
        for agent in agents:
            if agent.trip_id is not None:
                lat, lon, _ = _interpolate_route(
                    METRO_REGIONS[agent.region_id].routes[agent.route_id],
                    agent.route_progress,
                )
                agent.sequence_number += 1
                trip_rec = {
                    "event_id": deterministic_event_id(
                        cfg.seed, agent.vehicle_id, "trip_end", agent.sequence_number
                    ),
                    "vehicle_id": agent.vehicle_id,
                    "trip_id": agent.trip_id,
                    "event_type": "trip_end",
                    "event_time": end_time.isoformat(),
                    "ingest_time": datetime.now(UTC).isoformat(),
                    "sequence_number": agent.sequence_number,
                    "schema_version": "1.0.0",
                    "source_id": f"sim-node-{hash(agent.vehicle_id) % 4}",
                    "correlation_id": corr_id,
                    "trace_id": trace_id,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "region": agent.region_id,
                    "software_version": agent.software_version,
                    "odometer_km": round(agent.odometer_km, 3),
                    "battery_soc_pct": round(agent.battery_soc_pct, 2),
                    "route_id": agent.route_id,
                    "metadata": {"synthetic": "true", "sim_seed": str(cfg.seed)},
                }
                raw_bytes = self.registry.serialize("fleet.trips.raw", trip_rec)
                trip_end_batch.append(
                    (agent.vehicle_id, raw_bytes, int(end_time.timestamp() * 1000), None)
                )
                counts["trips_sent"] += 1
                counts["bytes_produced"] += len(raw_bytes)
        if trip_end_batch:
            self.bus.produce_batch("fleet.trips.raw", trip_end_batch)

        # Inject poison/corrupt payloads to verify DLQ quarantine path
        if cfg.poison_count > 0:
            poison_batch = [
                (
                    f"VEH-POISON-{i:03d}",
                    b"\xffBAD_AVRO_CORRUPT_PAYLOAD_" + str(i).encode("utf-8"),
                    int(end_time.timestamp() * 1000),
                    {"injected_poison": "true"},
                )
                for i in range(cfg.poison_count)
            ]
            self.bus.produce_batch("fleet.telemetry.raw", poison_batch)
            counts["poison_injected"] = cfg.poison_count

        duration_s = max(1e-6, time.perf_counter() - t0)
        total_events = (
            counts["telemetry_sent"]
            + counts["trips_sent"]
            + counts["autonomy_sent"]
            + counts["diagnostics_sent"]
            + counts["gps_sent"]
            + counts["poison_injected"]
        )
        summary: dict[str, Any] = {
            "pipeline_run_id": pipeline_run_id,
            "seed": cfg.seed,
            "virtual_fleet_size": cfg.virtual_fleet_size,
            "active_vehicles": cfg.active_vehicles,
            "ticks": cfg.ticks,
            "transport_backend": self.bus.backend_name,
            "duration_seconds": round(duration_s, 4),
            "total_events_produced": total_events,
            "throughput_events_per_sec": round(total_events / duration_s, 2),
            "throughput_bytes_per_sec": round(counts["bytes_produced"] / duration_s, 2),
            "counts": counts,
            "ground_truth_anomaly_count": len(ground_truth_anomalies),
            "ground_truth_anomalies": ground_truth_anomalies,
        }

        if cfg.ground_truth_output is not None:
            cfg.ground_truth_output.parent.mkdir(parents=True, exist_ok=True)
            cfg.ground_truth_output.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        self.logger.info(
            "simulator.run_complete",
            f"Produced {total_events} synthetic events across {cfg.active_vehicles} active vehicles "
            f"(virtual population: {cfg.virtual_fleet_size})",
            trace_id=trace_id,
            correlation_id=corr_id,
            pipeline_run_id=pipeline_run_id,
            context={
                "duration_seconds": summary["duration_seconds"],
                "throughput_eps": summary["throughput_events_per_sec"],
                "ground_truth_anomalies": len(ground_truth_anomalies),
                "poison_injected": counts["poison_injected"],
            },
        )
        return summary
