#!/usr/bin/env python3
"""CLI entrypoint for the synthetic autonomous fleet telemetry simulator."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fleet_platform.config import load_config
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.simulator import WORKLOAD_PROFILES, FleetSimulator, SimulationConfig
from fleet_platform.transport.factory import create_event_bus


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deterministic synthetic autonomous fleet telemetry simulator."
    )
    parser.add_argument(
        "--profile",
        choices=sorted(WORKLOAD_PROFILES.keys()),
        default="smoke",
        help="Workload sizing profile (smoke, local, stress, virtual-population)",
    )
    parser.add_argument("--virtual-fleet-size", type=int, default=None)
    parser.add_argument("--active-vehicles", type=int, default=None)
    parser.add_argument("--ticks", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--target-eps", type=float, default=None)
    parser.add_argument("--ground-truth-out", type=Path, default=None)
    args = parser.parse_args()

    cfg = load_config()
    bus = create_event_bus(cfg)
    registry = AvroContractRegistry()
    sim_cfg = SimulationConfig.from_profile(
        args.profile,
        virtual_fleet_size=args.virtual_fleet_size,
        active_vehicles=args.active_vehicles,
        ticks=args.ticks,
        seed=args.seed,
        target_events_per_sec=args.target_eps,
        ground_truth_output=args.ground_truth_out
        or (cfg.local_data_dir / "ground_truth_anomalies.json"),
    )
    sim = FleetSimulator(cfg, bus, registry, sim_cfg)
    summary = sim.run()
    # Print concise summary without dumping huge anomaly array to stdout
    brief = {k: v for k, v in summary.items() if k != "ground_truth_anomalies"}
    print(json.dumps(brief, indent=2))


if __name__ == "__main__":
    main()
