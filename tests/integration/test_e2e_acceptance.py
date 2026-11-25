"""End-to-end acceptance tests verifying Criteria B, C, D, E, F, H, I, J, K, and L."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from fleet_platform.batch_pipeline import MedallionBatchPipeline
from fleet_platform.cdc import CdcReferencePipeline
from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.observability import OpenLineageEmitter
from fleet_platform.quality.validator import DataQualityRunner
from fleet_platform.simulator import FleetSimulator, SimulationConfig
from fleet_platform.streaming.state_engine import StatefulStreamEngine
from fleet_platform.transport.local_wal import LocalWalEventBus


def _load_api_create_app():
    api_dir = Path(__file__).resolve().parents[2] / "apps" / "fleet-api"
    if str(api_dir) not in sys.path:
        sys.path.insert(0, str(api_dir))
    spec = importlib.util.spec_from_file_location("fleet_api_e2e", api_dir / "main.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.create_app


@pytest.mark.integration
def test_full_end_to_end_acceptance_pipeline(isolated_config: PlatformConfig) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    lh = IcebergLakehouse(isolated_config)
    lineage = OpenLineageEmitter(local_store_dir=isolated_config.lineage_dir)

    # Criterion B: Generate 1,000 synthetic vehicles and observe partitioned ingestion
    sim_cfg = SimulationConfig.from_profile("local", ticks=6, seed=42)
    sim_res = FleetSimulator(isolated_config, bus, reg, sim_cfg).run()
    assert sim_res["active_vehicles"] == 1000
    assert sim_res["counts"]["duplicates_injected"] > 0
    assert sim_res["counts"]["out_of_order_injected"] > 0
    assert sim_res["counts"]["poison_injected"] == 5

    # Criteria E & F: Stream processing detects known hard-braking burst + CEP, suppresses duplicates
    stream = StatefulStreamEngine(isolated_config, bus, reg)
    st_res = stream.process_available()
    assert st_res["duplicates_suppressed"] == sim_res["counts"]["duplicates_injected"]
    assert st_res["poison_skipped"] == 5
    assert st_res["anomalies_emitted"] > 0

    # Criterion C: Durable Bronze Iceberg ingestion + poison DLQ quarantine
    ingestor = BronzeIngestor(isolated_config, bus, reg, lh, lineage=lineage)
    ing_res = ingestor.drain_all_topics()
    assert ing_res["topics"]["fleet.telemetry.raw"]["quarantined_dlq"] == 5
    assert ing_res["topics"]["fleet.telemetry.raw"]["remaining_lag"] == 0

    # CDC SCD2 + Criterion D: Silver normalization & Gold aggregates
    cdc = CdcReferencePipeline(isolated_config, bus, reg, lh, lineage=lineage)
    cdc.publish_synthetic_cdc(num_vehicles=1000, seed=42)
    cdc.materialize_scd2()

    batch = MedallionBatchPipeline(isolated_config, lh, lineage=lineage)
    s_res = batch.run_bronze_to_silver()
    g_res = batch.run_silver_to_gold()
    assert s_res["duplicates_removed"] == sim_res["counts"]["duplicates_injected"]
    assert g_res["gold_vehicle_latest_rows"] == 1000

    # Criterion E verification: Known hard-braking burst + CEP on VEH-000001 with evidence
    v1_anoms = lh.query_sql(
        "SELECT rule_id, rule_version, severity, evidence_json, source_event_ids_csv "
        "FROM silver_anomalies WHERE vehicle_id = 'VEH-000001'"
    )
    rules_hit = {r["rule_id"] for r in v1_anoms}
    assert "RULE_HARD_BRAKING" in rules_hit
    assert "RULE_REPEATED_HARD_BRAKING_CEP" in rules_hit
    cep_row = next(r for r in v1_anoms if r["rule_id"] == "RULE_REPEATED_HARD_BRAKING_CEP")
    assert cep_row["severity"] == "CRITICAL"
    assert len(cep_row["source_event_ids_csv"].split(",")) >= 3

    # Criterion H: Replay bounded dataset -> idempotent Gold totals unchanged
    gold_events_before = lh.query_sql("SELECT SUM(total_events) AS cnt FROM gold_fleet_activity")[
        0
    ]["cnt"]
    replay_res = ingestor.replay_time_window(
        topic="fleet.telemetry.raw", replay_batch_id="acceptance-replay-001"
    )
    assert replay_res["committed"] > 0
    batch.run_bronze_to_silver()
    batch.run_silver_to_gold()
    gold_events_after_replay = lh.query_sql(
        "SELECT SUM(total_events) AS cnt FROM gold_fleet_activity"
    )[0]["cnt"]
    assert gold_events_after_replay == gold_events_before

    # Criterion I: Date-scoped backfill + full DQ reconciliation PASS
    batch.run_backfill(target_date="2026-11-24")
    dq_res = DataQualityRunner(isolated_config, lh).run_all_checks()
    assert dq_res["overall_status"] == "PASS"

    # Criteria J, K, L: Query Gold via API endpoints, verify Prometheus /metrics & OpenLineage
    app = _load_api_create_app()(isolated_config)
    client = TestClient(app)
    for ep in (
        "/health",
        "/fleet/summary",
        "/fleet/vehicles?limit=10",
        "/fleet/vehicles/VEH-000001",
        "/fleet/anomalies?limit=10",
        "/fleet/trips?limit=10",
        "/fleet/regions",
        "/fleet/data-quality",
        "/platform/metrics",
    ):
        resp = client.get(ep)
        assert resp.status_code == 200, f"{ep} failed: {resp.text}"
        assert resp.headers.get("X-Simulated-Data") == "true"

    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "fleet_api_requests_total" in metrics_resp.text
    assert len(lineage.read_local_events()) >= 4
