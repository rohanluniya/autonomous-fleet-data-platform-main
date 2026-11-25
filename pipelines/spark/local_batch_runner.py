#!/usr/bin/env python3
"""Local native Iceberg medallion batch runner (Bronze -> Silver -> Gold + SCD2 + DQ + Maintenance).

Executes the exact medallion business logic directly on Apache Iceberg Format V2 tables
via PyIceberg 0.12.0 + PyArrow + DuckDB 2.0.0 when running on zero-Docker hosts.
Containerized Spark 4.1.4 jobs live alongside this script in `pipelines/spark/`.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fleet_platform.batch_pipeline import MedallionBatchPipeline
from fleet_platform.cdc import CdcReferencePipeline
from fleet_platform.config import load_config
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.quality.validator import DataQualityRunner
from fleet_platform.transport.factory import create_event_bus


def main() -> None:
    parser = argparse.ArgumentParser(description="Run medallion lakehouse batch curation.")
    parser.add_argument("--seed-cdc-vehicles", type=int, default=0)
    parser.add_argument("--backfill-date", type=str, default=None)
    parser.add_argument("--run-maintenance", action="store_true")
    parser.add_argument("--run-dq", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    bus = create_event_bus(cfg)
    registry = AvroContractRegistry()
    lh = IcebergLakehouse(cfg)
    output: dict[str, object] = {}

    if args.seed_cdc_vehicles > 0:
        cdc = CdcReferencePipeline(cfg, bus, registry, lh)
        published = cdc.publish_synthetic_cdc(num_vehicles=args.seed_cdc_vehicles)
        output["cdc_published"] = published
        output["scd2"] = cdc.materialize_scd2()

    pipeline = MedallionBatchPipeline(cfg, lh)
    if args.backfill_date:
        output["backfill"] = pipeline.run_backfill(target_date=args.backfill_date)
    else:
        output["bronze_to_silver"] = pipeline.run_bronze_to_silver()
        output["silver_to_gold"] = pipeline.run_silver_to_gold()

    if args.run_maintenance:
        output["maintenance"] = pipeline.run_maintenance()

    if args.run_dq:
        dq = DataQualityRunner(cfg, lh)
        dq_res = dq.run_all_checks()
        output["data_quality"] = {
            "overall_status": dq_res["overall_status"],
            "summary": dq_res["summary"],
        }

    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
