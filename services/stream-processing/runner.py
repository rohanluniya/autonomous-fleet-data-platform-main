#!/usr/bin/env python3
"""CLI runner for the stateful event-time stream processor (local_native or Kafka mode)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fleet_platform.config import load_config
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.streaming.state_engine import StatefulStreamEngine, StreamEngineConfig
from fleet_platform.transport.factory import create_event_bus


def main() -> None:
    parser = argparse.ArgumentParser(description="Run stateful event-time stream processor.")
    parser.add_argument(
        "--restore", action="store_true", help="Restore prior checkpoint if present"
    )
    parser.add_argument("--max-records", type=int, default=200_000)
    parser.add_argument("--watermark-delay-s", type=float, default=30.0)
    args = parser.parse_args()

    cfg = load_config()
    bus = create_event_bus(cfg)
    registry = AvroContractRegistry()
    engine = StatefulStreamEngine(
        cfg,
        bus,
        registry,
        engine_config=StreamEngineConfig(max_out_of_orderness_seconds=args.watermark_delay_s),
    )
    if args.restore:
        engine.restore_checkpoint()
    summary = engine.process_available(max_records=args.max_records)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
