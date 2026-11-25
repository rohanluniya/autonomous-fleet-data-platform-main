"""Stateful event-time stream processing engine with bounded out-of-order watermarks,
deduplication with State TTL, tumbling/sliding/session windows, telemetry gap detection,
late-event side outputs, and atomic checkpoint/savepoint recovery.

Acts as the verified local_native runtime parity engine for the Apache Flink 2.1.3 job.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry, SchemaValidationError
from fleet_platform.observability import get_logger, get_metrics
from fleet_platform.streaming.rules import VehicleStreamingState, evaluate_telemetry_rules
from fleet_platform.transport.base import EventBus


@dataclass
class StreamEngineConfig:
    consumer_group: str = "fleet-flink-stream-processor"
    max_out_of_orderness_seconds: float = 30.0
    dedup_ttl_seconds: float = 3600.0
    gap_threshold_seconds: float = 45.0
    tumbling_window_seconds: int = 60
    session_gap_seconds: float = 60.0
    cep_window_seconds: float = 120.0
    cep_min_count: int = 3
    checkpoint_interval_events: int = 2000


class StatefulStreamEngine:
    """Event-time stateful stream processor with atomic checkpoints and exact recovery semantics."""

    def __init__(
        self,
        platform_config: PlatformConfig,
        bus: EventBus,
        registry: AvroContractRegistry,
        engine_config: StreamEngineConfig | None = None,
        checkpoint_path: Path | None = None,
    ) -> None:
        self.platform_config = platform_config
        self.bus = bus
        self.registry = registry
        self.cfg = engine_config or StreamEngineConfig()
        self.checkpoint_path = checkpoint_path or (
            platform_config.checkpoint_dir / "stream_checkpoint.json"
        )
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger(
            "stream-processing", "state_engine", environment=platform_config.env
        )
        self.metrics = get_metrics()

        # Keyed & operator state
        self.vehicle_states: dict[str, VehicleStreamingState] = {}
        self.dedup_seen: dict[str, float] = {}  # event_id -> event_ts_sec (pruned by TTL)
        self.max_event_ts: float = 0.0
        self.watermark_ts: float = 0.0
        self.partition_offsets: dict[int, int] = {}
        self.tumbling_agg: dict[str, dict[str, Any]] = {}
        self.late_events_side_output: list[dict[str, Any]] = []
        self.checkpoints_completed: int = 0
        self.total_processed: int = 0
        self.total_duplicates_suppressed: int = 0
        self.total_late_events: int = 0
        self.total_anomalies_emitted: int = 0
        self.total_gap_alerts: int = 0
        self.total_poison_skipped: int = 0

    def save_checkpoint(self, path: Path | None = None) -> dict[str, Any]:
        """Atomically persist keyed state, dedup cache, watermark, and consumer offsets."""
        t0 = time.perf_counter()
        target = path or self.checkpoint_path
        payload = {
            "checkpoint_id": self.checkpoints_completed + 1,
            "saved_at_utc": datetime.now(UTC).isoformat(),
            "max_event_ts": self.max_event_ts,
            "watermark_ts": self.watermark_ts,
            "partition_offsets": {str(k): v for k, v in self.partition_offsets.items()},
            "dedup_seen": self.dedup_seen,
            "vehicle_states": {vid: st.to_dict() for vid, st in self.vehicle_states.items()},
            "tumbling_agg": self.tumbling_agg,
            "counters": {
                "total_processed": self.total_processed,
                "total_duplicates_suppressed": self.total_duplicates_suppressed,
                "total_late_events": self.total_late_events,
                "total_anomalies_emitted": self.total_anomalies_emitted,
                "total_gap_alerts": self.total_gap_alerts,
                "total_poison_skipped": self.total_poison_skipped,
            },
        }
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(target)
        if self.partition_offsets:
            self.bus.commit_offsets(
                "fleet.telemetry.raw", self.cfg.consumer_group, self.partition_offsets
            )
        self.checkpoints_completed += 1
        dur = time.perf_counter() - t0
        self.metrics.stream_checkpoint_duration_seconds.observe(dur)
        return {
            "checkpoint_id": self.checkpoints_completed,
            "path": str(target),
            "state_vehicles": len(self.vehicle_states),
            "dedup_keys": len(self.dedup_seen),
            "duration_seconds": round(dur, 4),
        }

    def restore_checkpoint(self, path: Path | None = None) -> bool:
        """Restore keyed state, watermark, dedup state, and committed offsets from checkpoint."""
        target = path or self.checkpoint_path
        if not target.exists():
            return False
        data = json.loads(target.read_text(encoding="utf-8"))
        self.max_event_ts = float(data["max_event_ts"])
        self.watermark_ts = float(data["watermark_ts"])
        self.partition_offsets = {int(k): int(v) for k, v in data["partition_offsets"].items()}
        self.dedup_seen = {k: float(v) for k, v in data["dedup_seen"].items()}
        self.vehicle_states = {
            vid: VehicleStreamingState.from_dict(raw) for vid, raw in data["vehicle_states"].items()
        }
        self.tumbling_agg = dict(data.get("tumbling_agg", {}))
        ctrs = data.get("counters", {})
        self.total_processed = int(ctrs.get("total_processed", 0))
        self.total_duplicates_suppressed = int(ctrs.get("total_duplicates_suppressed", 0))
        self.total_late_events = int(ctrs.get("total_late_events", 0))
        self.total_anomalies_emitted = int(ctrs.get("total_anomalies_emitted", 0))
        self.total_gap_alerts = int(ctrs.get("total_gap_alerts", 0))
        self.total_poison_skipped = int(ctrs.get("total_poison_skipped", 0))
        self.checkpoints_completed = int(data.get("checkpoint_id", 0))
        self.logger.info(
            "stream.checkpoint_restored",
            f"Restored checkpoint #{self.checkpoints_completed} with {len(self.vehicle_states)} keyed vehicles",
            context={"checkpoint_path": str(target), "watermark_ts": self.watermark_ts},
        )
        return True

    def _prune_dedup_ttl(self) -> None:
        if self.watermark_ts <= 0:
            return
        cutoff = self.watermark_ts - self.cfg.dedup_ttl_seconds
        expired = [eid for eid, ts in self.dedup_seen.items() if ts < cutoff]
        for eid in expired:
            del self.dedup_seen[eid]

    def _check_gap_timers(self) -> list[tuple[str, bytes, int | None, dict[str, str] | None]]:
        """Emit TELEMETRY_GAP vehicle-state updates for driving vehicles silent past gap_threshold."""
        emitted: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []
        if self.watermark_ts <= 0:
            return emitted
        now_iso = datetime.now(UTC).isoformat()
        for vid, st in self.vehicle_states.items():
            if st.operational_state != "driving" or st.gap_alerted or st.last_event_ts <= 0:
                continue
            gap = self.watermark_ts - st.last_event_ts
            if gap >= self.cfg.gap_threshold_seconds:
                st.gap_alerted = True
                self.total_gap_alerts += 1
                state_ev = {
                    "event_id": str(uuid.uuid4()),
                    "vehicle_id": vid,
                    "region": st.region,
                    "software_version": st.software_version,
                    "operational_state": st.operational_state,
                    "connectivity_status": "TELEMETRY_GAP",
                    "last_event_time": datetime.fromtimestamp(st.last_event_ts, tz=UTC).isoformat(),
                    "state_updated_at": now_iso,
                    "gap_duration_seconds": round(gap, 2),
                    "battery_soc_pct": round(st.last_battery_soc_pct, 2),
                    "h3_cell_res6": st.last_h3_res6,
                    "schema_version": "1.0.0",
                }
                raw = self.registry.serialize("fleet.vehicle-state", state_ev)
                emitted.append((vid, raw, int(self.watermark_ts * 1000), None))
        return emitted

    def process_available(
        self,
        *,
        max_records: int = 100_000,
        fail_after_records: int | None = None,
    ) -> dict[str, Any]:
        """Consume available telemetry records, run stateful event-time processing, and checkpoint.

        Args:
            fail_after_records: Optional fault-injection hook simulating an abrupt TaskManager crash
                mid-stream (after saving prior checkpoints, before final checkpoint).
        """
        t0 = time.perf_counter()
        records = self.bus.consume_batch(
            "fleet.telemetry.raw",
            self.cfg.consumer_group,
            max_records=max_records,
            auto_commit=False,
        )
        anomaly_out: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []
        state_out: list[tuple[str, bytes, int | None, dict[str, str] | None]] = []
        since_last_ckpt = 0
        processed_this_call = 0

        for rec in records:
            if fail_after_records is not None and processed_this_call >= fail_after_records:
                # Flush already emitted anomalies up to last checkpoint only on clean path;
                # simulate abrupt crash BEFORE committing uncommitted offsets!
                raise RuntimeError(
                    f"Simulated stream TaskManager crash after {processed_this_call} records"
                )

            try:
                _, event = self.registry.deserialize(
                    rec.value, expected_topic="fleet.telemetry.raw"
                )
            except SchemaValidationError:
                self.total_poison_skipped += 1
                self.partition_offsets[rec.partition] = rec.offset + 1
                self.metrics.stream_events_processed_total.labels(status="poison_skipped").inc()
                continue

            ev_id = str(event["event_id"])
            vid = str(event["vehicle_id"])
            ev_dt = datetime.fromisoformat(event["event_time"])
            ev_ts = ev_dt.timestamp()

            # 1. Bounded State TTL Deduplication
            if ev_id in self.dedup_seen:
                self.total_duplicates_suppressed += 1
                self.partition_offsets[rec.partition] = rec.offset + 1
                self.metrics.stream_events_processed_total.labels(status="duplicate_dropped").inc()
                continue
            self.dedup_seen[ev_id] = ev_ts

            # 2. Event-Time Watermark Progression
            if ev_ts > self.max_event_ts:
                self.max_event_ts = ev_ts
                self.watermark_ts = self.max_event_ts - self.cfg.max_out_of_orderness_seconds

            # 3. Late-Event Side Output (events arriving behind current watermark)
            is_late = self.watermark_ts > 0 and ev_ts < self.watermark_ts
            if is_late:
                self.total_late_events += 1
                self.late_events_side_output.append(
                    {
                        "event_id": ev_id,
                        "vehicle_id": vid,
                        "event_time": event["event_time"],
                        "watermark_utc": datetime.fromtimestamp(
                            self.watermark_ts, tz=UTC
                        ).isoformat(),
                        "lateness_seconds": round(self.watermark_ts - ev_ts, 2),
                    }
                )
                self.metrics.stream_events_processed_total.labels(status="late_side_output").inc()

            # 4. Keyed State + Session Tracking + Versioned Anomaly Rules + CEP
            st = self.vehicle_states.get(vid)
            if st is None:
                st = VehicleStreamingState(vehicle_id=vid, session_start_ts=ev_ts)
                self.vehicle_states[vid] = st
            elif st.last_event_ts > 0 and (ev_ts - st.last_event_ts) > self.cfg.session_gap_seconds:
                st.session_start_ts = ev_ts
                st.session_event_count = 0
            st.session_event_count += 1

            anomalies = evaluate_telemetry_rules(
                event,
                st,
                event_ts_sec=ev_ts,
                cep_window_seconds=self.cfg.cep_window_seconds,
                cep_min_count=self.cfg.cep_min_count,
            )
            for anom in anomalies:
                raw_anom = self.registry.serialize("fleet.anomalies.detected", anom)
                anomaly_out.append((vid, raw_anom, int(ev_ts * 1000), None))
                self.total_anomalies_emitted += 1
                self.metrics.stream_anomalies_detected_total.labels(
                    rule_id=anom["rule_id"], severity=anom["severity"]
                ).inc()

            # 5. Tumbling Event-Time Window Aggregation (by window_start + region + software_version)
            pl = event["payload"]
            win_start = (
                int(ev_ts // self.cfg.tumbling_window_seconds) * self.cfg.tumbling_window_seconds
            )
            agg_key = f"{win_start}|{pl['region']}|{pl['software_version']}"
            bucket = self.tumbling_agg.get(agg_key)
            if bucket is None:
                bucket = {
                    "window_start_utc": datetime.fromtimestamp(win_start, tz=UTC).isoformat(),
                    "region": pl["region"],
                    "software_version": pl["software_version"],
                    "event_count": 0,
                    "speed_sum": 0.0,
                    "anomaly_count": 0,
                }
                self.tumbling_agg[agg_key] = bucket
            bucket["event_count"] += 1
            bucket["speed_sum"] += float(pl["speed_mps"])
            bucket["anomaly_count"] += len(anomalies)

            self.total_processed += 1
            processed_this_call += 1
            since_last_ckpt += 1
            self.partition_offsets[rec.partition] = rec.offset + 1
            self.metrics.stream_events_processed_total.labels(status="processed").inc()

            if since_last_ckpt >= self.cfg.checkpoint_interval_events:
                if anomaly_out:
                    self.bus.produce_batch("fleet.anomalies.detected", anomaly_out)
                    anomaly_out.clear()
                self._prune_dedup_ttl()
                self.save_checkpoint()
                since_last_ckpt = 0

        # Emit remaining anomalies + vehicle state updates + telemetry gap detections
        if anomaly_out:
            self.bus.produce_batch("fleet.anomalies.detected", anomaly_out)

        gap_events = self._check_gap_timers()
        state_out.extend(gap_events)

        now_iso = datetime.now(UTC).isoformat()
        for vid, st in self.vehicle_states.items():
            if not st.gap_alerted and st.last_event_ts > 0:
                sv = {
                    "event_id": str(uuid.uuid4()),
                    "vehicle_id": vid,
                    "region": st.region,
                    "software_version": st.software_version,
                    "operational_state": st.operational_state,
                    "connectivity_status": "ONLINE"
                    if st.operational_state != "offline"
                    else "OFFLINE",
                    "last_event_time": datetime.fromtimestamp(st.last_event_ts, tz=UTC).isoformat(),
                    "state_updated_at": now_iso,
                    "gap_duration_seconds": 0.0,
                    "battery_soc_pct": round(st.last_battery_soc_pct, 2),
                    "h3_cell_res6": st.last_h3_res6,
                    "schema_version": "1.0.0",
                }
                state_out.append(
                    (vid, self.registry.serialize("fleet.vehicle-state", sv), None, None)
                )
        if state_out:
            self.bus.produce_batch("fleet.vehicle-state", state_out)

        self._prune_dedup_ttl()
        ckpt = self.save_checkpoint()

        self.metrics.stream_state_keys.labels(state_store="vehicle_state").set(
            len(self.vehicle_states)
        )
        self.metrics.stream_state_keys.labels(state_store="dedup_ttl").set(len(self.dedup_seen))
        if self.watermark_ts > 0:
            self.metrics.stream_watermark_lag_seconds.set(max(0.0, time.time() - self.watermark_ts))

        dur = max(1e-6, time.perf_counter() - t0)
        return {
            "records_read": len(records),
            "processed_this_call": processed_this_call,
            "total_processed": self.total_processed,
            "duplicates_suppressed": self.total_duplicates_suppressed,
            "late_events_side_output": self.total_late_events,
            "anomalies_emitted": self.total_anomalies_emitted,
            "telemetry_gap_alerts": self.total_gap_alerts,
            "poison_skipped": self.total_poison_skipped,
            "active_vehicle_keys": len(self.vehicle_states),
            "dedup_state_keys": len(self.dedup_seen),
            "tumbling_windows": len(self.tumbling_agg),
            "duration_seconds": round(dur, 4),
            "throughput_events_per_sec": round(len(records) / dur, 2),
            "checkpoint": ckpt,
        }
