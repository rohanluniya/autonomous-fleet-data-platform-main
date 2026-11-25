#!/usr/bin/env python3
"""CLI entrypoint for durable Kafka/WAL -> Apache Iceberg Bronze ingestion."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fleet_platform.config import load_config
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.transport.factory import create_event_bus


def main() -> None:
    cfg = load_config()
    bus = create_event_bus(cfg)
    registry = AvroContractRegistry()
    lakehouse = IcebergLakehouse(cfg)
    ingestor = BronzeIngestor(cfg, bus, registry, lakehouse)
    result = ingestor.drain_all_topics()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
