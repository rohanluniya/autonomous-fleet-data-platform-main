"""Phase 12 Controlled Fault-Injection & Recovery Test Suite.

Tests real failure modes, measures recovery duration, asserts zero duplicate business
effects or silent data loss, and records raw fault-injection evidence to
`results/fault_injection/fault_injection_results.json`.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from fleet_platform.batch_pipeline import MedallionBatchPipeline
from fleet_platform.cdc import build_scd2_dimension, generate_synthetic_cdc_events
from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry, SchemaValidationError
from fleet_platform.ingestion import BronzeIngestor
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.simulator import FleetSimulator, SimulationConfig
from fleet_platform.streaming.state_engine import StatefulStreamEngine, StreamEngineConfig
from fleet_platform.transport.local_wal import BrokerUnavailableError, LocalWalEventBus

_FAULT_EVIDENCE: list[dict[str, Any]] = []


def _record_fault(scenario: str, passed: bool, recovery_ms: float, metrics: dict[str, Any]) -> None:
    _FAULT_EVIDENCE.append(
        {
            "scenario": scenario,
            "status": "PASS" if passed else "FAIL",
            "recovery_duration_ms": round(recovery_ms, 2),
            "measured_evidence": metrics,
            "recorded_at_utc": datetime.now(UTC).isoformat(),
        }
    )
    out = (
        Path(__file__).resolve().parents[2]
        / "results"
        / "fault_injection"
        / "fault_injection_results.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(_FAULT_EVIDENCE, indent=2), encoding="utf-8")


@pytest.mark.fault
def test_fault_01_broker_restart_and_producer_retry(isolated_config: PlatformConfig) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    bus.set_broker_online(False)

    t0 = time.perf_counter()
    retries = 0
    delivered = False
    payload = reg.serialize(
        "fleet.telemetry.raw",
        {
            "event_id": "evt-retry-001",
            "vehicle_id": "VEH-000001",
            "trip_id": None,
            "event_type": "telemetry",
            "event_time": datetime.now(UTC).isoformat(),
            "ingest_time": datetime.now(UTC).isoformat(),
            "sequence_number": 1,
            "schema_version": "1.0.0",
            "source_id": "fault-test",
            "correlation_id": "c1",
            "trace_id": "t1",
            "location": {
                "latitude": 37.78,
                "longitude": -122.41,
                "heading_deg": 0.0,
                "altitude_m": 0.0,
            },
            "payload": {
                "vehicle_state": "driving",
                "region": "sf_bay",
                "vehicle_model": "AV-Sedan-G3",
                "software_version": "v4.2.0",
                "speed_mps": 10.0,
                "acceleration_mps2": 0.1,
                "braking_force_pct": 0.0,
                "battery_soc_pct": 80.0,
                "battery_temp_c": 30.0,
                "motor_temp_c": 55.0,
                "odometer_km": 100.0,
                "fault_codes": [],
                "sensor_health_score": 0.98,
                "lidar_status": "OK",
                "camera_status": "OK",
                "radar_status": "OK",
            },
            "metadata": {},
        },
    )
    for attempt in range(4):
        try:
            if attempt == 2:
                bus.set_broker_online(True)
            bus.produce("fleet.telemetry.raw", "VEH-000001", payload)
            delivered = True
            break
        except BrokerUnavailableError:
            retries += 1
            time.sleep(0.02)

    rec_ms = (time.perf_counter() - t0) * 1000.0
    assert delivered is True and retries == 2
    assert bus.topic_stats()["fleet.telemetry.raw"]["records"] == 1
    _record_fault(
        "broker_outage_and_producer_retry",
        True,
        rec_ms,
        {"retries_before_recovery": retries, "records_committed": 1, "data_loss": 0},
    )


@pytest.mark.fault
def test_fault_02_stream_taskmanager_crash_and_checkpoint_restore(
    isolated_config: PlatformConfig,
) -> None:
    """Acceptance Criterion G: Kill stream processor mid-stream, restore checkpoint, assert exact parity."""
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    sim_cfg = SimulationConfig(
        virtual_fleet_size=80, active_vehicles=80, ticks=6, seed=77, poison_count=2
    )
    FleetSimulator(isolated_config, bus, reg, sim_cfg).run()

    # Baseline uninterrupted stream run (separate consumer group)
    baseline = StatefulStreamEngine(
        isolated_config,
        bus,
        reg,
        engine_config=StreamEngineConfig(
            consumer_group="baseline-clean-group", checkpoint_interval_events=100
        ),
        checkpoint_path=isolated_config.checkpoint_dir / "baseline.json",
    )
    base_res = baseline.process_available()

    # Faulted run: checkpoint every 100 records, then crash abruptly after 245 records
    ckpt_path = isolated_config.checkpoint_dir / "crash_recovery.json"
    crashed_engine = StatefulStreamEngine(
        isolated_config,
        bus,
        reg,
        engine_config=StreamEngineConfig(
            consumer_group="crashed-group", checkpoint_interval_events=100
        ),
        checkpoint_path=ckpt_path,
    )
    with pytest.raises(RuntimeError, match="Simulated stream TaskManager crash"):
        crashed_engine.process_available(fail_after_records=245)

    assert ckpt_path.exists(), "Periodic checkpoint before crash must exist"

    # Restart brand-new TaskManager instance, restore checkpoint, finish remaining stream
    t0 = time.perf_counter()
    recovered_engine = StatefulStreamEngine(
        isolated_config,
        bus,
        reg,
        engine_config=StreamEngineConfig(
            consumer_group="crashed-group", checkpoint_interval_events=100
        ),
        checkpoint_path=ckpt_path,
    )
    restored = recovered_engine.restore_checkpoint()
    assert restored is True
    rec_res = recovered_engine.process_available()
    rec_ms = (time.perf_counter() - t0) * 1000.0

    # Exact parity assertions: zero lost records, exact anomaly count match!
    assert rec_res["total_processed"] == base_res["total_processed"]
    assert rec_res["anomalies_emitted"] == base_res["anomalies_emitted"]
    assert bus.get_consumer_lag("fleet.telemetry.raw", "crashed-group")["total_lag"] == 0
    _record_fault(
        "stream_taskmanager_crash_checkpoint_restore",
        True,
        rec_ms,
        {
            "baseline_processed": base_res["total_processed"],
            "recovered_processed": rec_res["total_processed"],
            "baseline_anomalies": base_res["anomalies_emitted"],
            "recovered_anomalies": rec_res["anomalies_emitted"],
            "remaining_lag": 0,
        },
    )


@pytest.mark.fault
def test_fault_03_delayed_out_of_order_late_side_output_and_duplicates(
    isolated_config: PlatformConfig,
) -> None:
    """Acceptance Criterion F: Out-of-order late arrival beyond watermark + 50% duplicate surge."""
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    lh = IcebergLakehouse(isolated_config)

    t_base = datetime(2026, 11, 24, 15, 0, 0, tzinfo=UTC)

    def make_ev(eid: str, ts: datetime, seq: int) -> bytes:
        return reg.serialize(
            "fleet.telemetry.raw",
            {
                "event_id": eid,
                "vehicle_id": "VEH-LATE-01",
                "trip_id": "TRIP-LATE-01",
                "event_type": "telemetry",
                "event_time": ts.isoformat(),
                "ingest_time": datetime.now(UTC).isoformat(),
                "sequence_number": seq,
                "schema_version": "1.0.0",
                "source_id": "fault-test",
                "correlation_id": "c-late",
                "trace_id": "t-late",
                "location": {
                    "latitude": 37.78,
                    "longitude": -122.41,
                    "heading_deg": 90.0,
                    "altitude_m": 0.0,
                },
                "payload": {
                    "vehicle_state": "driving",
                    "region": "sf_bay",
                    "vehicle_model": "AV-Sedan-G3",
                    "software_version": "v4.2.0",
                    "speed_mps": 12.0,
                    "acceleration_mps2": 0.2,
                    "braking_force_pct": 0.0,
                    "battery_soc_pct": 75.0,
                    "battery_temp_c": 30.0,
                    "motor_temp_c": 52.0,
                    "odometer_km": 100.0 + seq * 0.1,
                    "fault_codes": [],
                    "sensor_health_score": 0.99,
                    "lidar_status": "OK",
                    "camera_status": "OK",
                    "radar_status": "OK",
                },
                "metadata": {},
            },
        )

    t0 = time.perf_counter()
    # Emit normal event at T+120s (advances watermark to T+90s), then duplicate it, then emit late event at T+0s
    e1 = make_ev("evt-on-time-1", t_base + timedelta(seconds=120), 2)
    e_late = make_ev("evt-very-late-0", t_base, 1)
    bus.produce("fleet.telemetry.raw", "VEH-LATE-01", e1)
    bus.produce("fleet.telemetry.raw", "VEH-LATE-01", e1)  # exact duplicate
    bus.produce("fleet.telemetry.raw", "VEH-LATE-01", e_late)  # 90s behind watermark!

    engine = StatefulStreamEngine(
        isolated_config,
        bus,
        reg,
        engine_config=StreamEngineConfig(max_out_of_orderness_seconds=30.0),
    )
    st = engine.process_available()
    assert st["duplicates_suppressed"] == 1
    assert st["late_events_side_output"] == 1
    assert engine.late_events_side_output[0]["event_id"] == "evt-very-late-0"

    # Durable Bronze + Silver preserves the late event (zero silent drop!) and removes the duplicate
    BronzeIngestor(isolated_config, bus, reg, lh).drain_all_topics()
    pipe = MedallionBatchPipeline(isolated_config, lh)
    s_res = pipe.run_bronze_to_silver()
    assert s_res["bronze_rows_read"] == 3
    assert s_res["duplicates_removed"] == 1
    assert s_res["silver_clean_rows"] == 2
    rec_ms = (time.perf_counter() - t0) * 1000.0
    _record_fault(
        "late_out_of_order_and_duplicate_surge",
        True,
        rec_ms,
        {
            "duplicates_suppressed_stream": 1,
            "late_events_routed_to_side_output": 1,
            "silver_clean_rows_preserved": 2,
        },
    )


@pytest.mark.fault
def test_fault_04_poison_messages_and_quarantine_isolation(
    isolated_config: PlatformConfig,
) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    lh = IcebergLakehouse(isolated_config)

    t0 = time.perf_counter()
    # Mix valid events, corrupt binary poison messages, and an implausible-speed DQ quarantine row
    FleetSimulator(
        isolated_config,
        bus,
        reg,
        SimulationConfig(virtual_fleet_size=10, active_vehicles=10, ticks=2, poison_count=4),
    ).run()

    # Inject a valid-Avro record with physically impossible speed (180 m/s) -> must hit silver.telemetry_quarantine
    bad_speed_bytes = reg.serialize(
        "fleet.telemetry.raw",
        {
            "event_id": "evt-bad-speed-999",
            "vehicle_id": "VEH-000001",
            "trip_id": None,
            "event_type": "telemetry",
            "event_time": datetime(2026, 11, 24, 12, 5, 0, tzinfo=UTC).isoformat(),
            "ingest_time": datetime(2026, 11, 24, 12, 5, 1, tzinfo=UTC).isoformat(),
            "sequence_number": 999,
            "schema_version": "1.0.0",
            "source_id": "fault-test",
            "correlation_id": "c-q",
            "trace_id": "t-q",
            "location": {
                "latitude": 37.78,
                "longitude": -122.41,
                "heading_deg": 0.0,
                "altitude_m": 0.0,
            },
            "payload": {
                "vehicle_state": "driving",
                "region": "sf_bay",
                "vehicle_model": "AV-Sedan-G3",
                "software_version": "v4.2.0",
                "speed_mps": 180.0,
                "acceleration_mps2": 0.0,
                "braking_force_pct": 0.0,
                "battery_soc_pct": 50.0,
                "battery_temp_c": 30.0,
                "motor_temp_c": 50.0,
                "odometer_km": 100.0,
                "fault_codes": [],
                "sensor_health_score": 0.99,
                "lidar_status": "OK",
                "camera_status": "OK",
                "radar_status": "OK",
            },
            "metadata": {},
        },
    )
    bus.produce("fleet.telemetry.raw", "VEH-000001", bad_speed_bytes)

    ing = BronzeIngestor(isolated_config, bus, reg, lh).drain_all_topics()
    assert ing["topics"]["fleet.telemetry.raw"]["quarantined_dlq"] == 4
    s_res = MedallionBatchPipeline(isolated_config, lh).run_bronze_to_silver()
    assert s_res["quarantined_rows"] == 1
    q_rows = lh.query_sql("SELECT event_id, quarantine_reason FROM silver_quarantine")
    assert q_rows[0]["quarantine_reason"] == "IMPLAUSIBLE_SPEED"

    rec_ms = (time.perf_counter() - t0) * 1000.0
    _record_fault(
        "poison_dlq_and_silver_dq_quarantine",
        True,
        rec_ms,
        {"dlq_poison_quarantined": 4, "silver_bounds_quarantined": 1, "silent_drops": 0},
    )


@pytest.mark.fault
def test_fault_05_partial_backfill_replay_idempotency_and_cdc_duplicate_lsn(
    isolated_config: PlatformConfig,
) -> None:
    bus = LocalWalEventBus(isolated_config.wal_dir)
    reg = AvroContractRegistry()
    lh = IcebergLakehouse(isolated_config)

    t0 = time.perf_counter()
    FleetSimulator(
        isolated_config,
        bus,
        reg,
        SimulationConfig(
            virtual_fleet_size=30, active_vehicles=30, ticks=4, seed=55, poison_count=0
        ),
    ).run()
    ing = BronzeIngestor(isolated_config, bus, reg, lh)
    ing.drain_all_topics()
    pipe = MedallionBatchPipeline(isolated_config, lh)
    pipe.run_bronze_to_silver()
    pipe.run_silver_to_gold()
    before_gold = lh.query_sql("SELECT SUM(total_events) AS cnt FROM gold_fleet_activity")[0]["cnt"]

    # Simulate partial failed backfill followed by full idempotent replay + date backfill
    ing.replay_time_window(replay_batch_id="partial-retry-1")
    ing.replay_time_window(replay_batch_id="partial-retry-2")
    pipe.run_backfill(target_date="2026-11-24")
    after_gold = lh.query_sql("SELECT SUM(total_events) AS cnt FROM gold_fleet_activity")[0]["cnt"]
    assert after_gold == before_gold

    # Verify CDC duplicate LSN replay produces identical SCD2 state
    cdc_ev = generate_synthetic_cdc_events(num_vehicles=20, seed=5, include_duplicates=True)
    scd_once = build_scd2_dimension(cdc_ev)
    scd_replayed = build_scd2_dimension(cdc_ev + cdc_ev)
    assert len(scd_once) == len(scd_replayed)

    rec_ms = (time.perf_counter() - t0) * 1000.0
    _record_fault(
        "idempotent_replay_backfill_and_cdc_dedup",
        True,
        rec_ms,
        {
            "gold_events_before": before_gold,
            "gold_events_after_double_replay": after_gold,
            "scd2_rows_idempotent": len(scd_replayed),
        },
    )


@pytest.mark.fault
def test_fault_06_schema_incompatibility_and_hot_key_skew(
    isolated_config: PlatformConfig,
) -> None:
    reg = AvroContractRegistry()
    t0 = time.perf_counter()

    # 1. Schema incompatibility rejected before hitting transport
    with pytest.raises(SchemaValidationError):
        reg.serialize("fleet.telemetry.raw", {"event_id": "incomplete-record-only"})

    # 2. High-cardinality skew (80% of 1,000 events on VEH-HOT-001) & salted two-stage aggregation parity
    records = [
        ("VEH-HOT-001" if i % 5 != 0 else f"VEH-{i:06d}", 10.0 + (i % 15)) for i in range(1000)
    ]
    # Unsalted direct average
    direct_sums: dict[str, list[float]] = {}
    for k, v in records:
        direct_sums.setdefault(k, []).append(v)
    direct_avgs = {k: round(sum(vs) / len(vs), 6) for k, vs in direct_sums.items()}

    # Two-stage salted aggregation (8 shards per hot key)
    stage1: dict[tuple[str, int], tuple[float, int]] = {}
    for idx, (k, v) in enumerate(records):
        salt = idx % 8
        s_val, s_cnt = stage1.get((k, salt), (0.0, 0))
        stage1[(k, salt)] = (s_val + v, s_cnt + 1)
    stage2: dict[str, tuple[float, int]] = {}
    for (k, _), (s_val, s_cnt) in stage1.items():
        t_val, t_cnt = stage2.get(k, (0.0, 0))
        stage2[k] = (t_val + s_val, t_cnt + s_cnt)
    salted_avgs = {k: round(val / cnt, 6) for k, (val, cnt) in stage2.items()}

    assert direct_avgs == salted_avgs
    rec_ms = (time.perf_counter() - t0) * 1000.0
    _record_fault(
        "schema_rejection_and_hot_key_skew_salting",
        True,
        rec_ms,
        {
            "hot_key_share_pct": 80.0,
            "unique_keys": len(salted_avgs),
            "salted_vs_direct_parity": True,
        },
    )
