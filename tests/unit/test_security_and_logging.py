"""Unit tests for RBAC authentication, location coarsening, and structured JSON log redaction."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from fleet_platform.batch_pipeline import MedallionBatchPipeline
from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.observability.logging import get_logger
from fleet_platform.simulator import FleetSimulator, SimulationConfig
from fleet_platform.transport.local_wal import LocalWalEventBus


def _load_api_create_app():
    api_dir = Path(__file__).resolve().parents[2] / "apps" / "fleet-api"
    if str(api_dir) not in sys.path:
        sys.path.insert(0, str(api_dir))
    spec = importlib.util.spec_from_file_location("fleet_api_main", api_dir / "main.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.create_app


@pytest.mark.unit
def test_structured_logger_schema_and_secret_redaction() -> None:
    logger = get_logger("test-svc", "security-test", environment="test")
    rec = logger.info(
        "test.auth_check",
        "Checking secret scrubbing",
        context={
            "vehicle_id": "VEH-000001",
            "authorization": "Bearer super-secret-token",
            "nested": {"api_key": "sk-12345", "safe": "ok"},
        },
    )
    for field in (
        "timestamp",
        "service",
        "environment",
        "severity",
        "trace_id",
        "correlation_id",
        "pipeline_run_id",
        "component",
        "event_type",
        "error_code",
        "retry_count",
    ):
        assert field in rec
    assert rec["context"]["authorization"] == "[REDACTED]"
    assert rec["context"]["nested"]["api_key"] == "[REDACTED]"
    assert rec["context"]["nested"]["safe"] == "ok"


@pytest.mark.unit
def test_api_rbac_and_spatial_privacy(isolated_config: PlatformConfig) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    lh = IcebergLakehouse(isolated_config)
    FleetSimulator(
        isolated_config,
        bus,
        reg,
        SimulationConfig(virtual_fleet_size=15, active_vehicles=15, ticks=3, poison_count=0),
    ).run()
    BronzeIngestor(isolated_config, bus, reg, lh).drain_all_topics()
    pipe = MedallionBatchPipeline(isolated_config, lh)
    pipe.run_bronze_to_silver()
    pipe.run_silver_to_gold()

    create_app = _load_api_create_app()
    app = create_app(isolated_config)
    client = TestClient(app)

    # Unauthenticated viewer in dev mode gets strictly coarsened H3 location
    r_viewer = client.get("/fleet/vehicles/VEH-000001")
    assert r_viewer.status_code == 200
    v_viewer = r_viewer.json()["vehicle"]
    assert v_viewer["location_precision"] == "coarsened_h3_res6"

    # Analyst bearer token also gets coarsened location
    r_analyst = client.get(
        "/fleet/vehicles/VEH-000001",
        headers={"Authorization": f"Bearer {isolated_config.api_analyst_token}"},
    )
    assert r_analyst.status_code == 200
    assert r_analyst.json()["vehicle"]["location_precision"] == "coarsened_h3_res6"

    # Admin bearer token gets exact raw GPS
    r_admin = client.get(
        "/fleet/vehicles/VEH-000001",
        headers={"Authorization": f"Bearer {isolated_config.api_admin_token}"},
    )
    assert r_admin.status_code == 200
    assert r_admin.json()["vehicle"]["location_precision"] == "raw_gps_authorized"

    # Invalid bearer token rejected with 403
    r_bad = client.get(
        "/fleet/vehicles/VEH-000001",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert r_bad.status_code == 403

    # Staging/prod mode rejects unauthenticated calls with 401
    prod_cfg = PlatformConfig(
        env="prod",
        local_data_dir=isolated_config.local_data_dir,
        warehouse_dir=isolated_config.warehouse_dir,
        wal_dir=isolated_config.wal_dir,
        checkpoint_dir=isolated_config.checkpoint_dir,
        lineage_dir=isolated_config.lineage_dir,
        sqlite_catalog_path=isolated_config.sqlite_catalog_path,
    )
    prod_client = TestClient(create_app(prod_cfg))
    assert prod_client.get("/fleet/summary").status_code == 401
