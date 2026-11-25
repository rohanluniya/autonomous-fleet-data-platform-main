#!/usr/bin/env python3
"""Bounded replay utility for re-ingesting a time slice into Bronze with replay provenance."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fleet_platform.config import load_config
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.transport.factory import create_event_bus


def _iso_to_ms(val: str | None) -> int | None:
    if not val:
        return None
    return int(datetime.fromisoformat(val).timestamp() * 1000)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay a bounded event window into Iceberg Bronze with provenance tagging."
    )
    parser.add_argument("--topic", default="fleet.telemetry.raw")
    parser.add_argument("--start-iso", default=None, help="Inclusive UTC ISO-8601 start timestamp")
    parser.add_argument("--end-iso", default=None, help="Inclusive UTC ISO-8601 end timestamp")
    parser.add_argument("--replay-batch-id", default=None)
    args = parser.parse_args()

    cfg = load_config()
    bus = create_event_bus(cfg)
    registry = AvroContractRegistry()
    lakehouse = IcebergLakehouse(cfg)
    ingestor = BronzeIngestor(cfg, bus, registry, lakehouse)
    res = ingestor.replay_time_window(
        topic=args.topic,
        start_ts_ms=_iso_to_ms(args.start_iso),
        end_ts_ms=_iso_to_ms(args.end_iso),
        replay_batch_id=args.replay_batch_id,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
