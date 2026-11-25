"""Durable raw ingestion from Kafka/WAL into Apache Iceberg Bronze tables with DLQ quarantine
and bounded time-range replay support.

Transactional semantics:
- At-least-once Bronze append: micro-batch is committed atomically to Iceberg Bronze first,
  and consumer offsets are committed ONLY after the Iceberg snapshot succeeds.
- Poison/schema-invalid payloads are never dropped silently; they are quarantined both to
  the `fleet.telemetry.dlq` topic and to `bronze.telemetry_dlq`.
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from fleet_platform.config import PlatformConfig
from fleet_platform.contracts.registry import AvroContractRegistry, SchemaValidationError
from fleet_platform.lakehouse.iceberg_store import IcebergLakehouse
from fleet_platform.observability import (
    OpenLineageEmitter,
    get_logger,
    get_metrics,
    new_trace_context,
)
from fleet_platform.transport.base import EventBus, TransportRecord


def _parse_utc(ts_str: str | None) -> datetime:
    if not ts_str:
        return datetime.now(UTC).replace(tzinfo=None)
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


class BronzeIngestor:
    """Consumes raw event streams, enforces Avro contracts, routes poison messages to DLQ,
    and commits immutable raw evidence to Apache Iceberg Bronze tables.
    """

    CONSUMER_GROUP = "fleet-bronze-ingestor"

    def __init__(
        self,
        config: PlatformConfig,
        bus: EventBus,
        registry: AvroContractRegistry,
        lakehouse: IcebergLakehouse,
        lineage: OpenLineageEmitter | None = None,
    ) -> None:
        self.config = config
        self.bus = bus
        self.registry = registry
        self.lakehouse = lakehouse
        self.lineage = lineage or OpenLineageEmitter(
            namespace=config.openlineage_namespace,
            marquez_url=config.openlineage_url,
            local_store_dir=config.lineage_dir,
        )
        self.logger = get_logger("bronze-ingestion", "ingestor", environment=config.env)
        self.metrics = get_metrics()

    def _quarantine_record(
        self,
        rec: TransportRecord,
        error_code: str,
        error_message: str,
        corr_id: str,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        dlq_payload = {
            "dlq_id": str(uuid.uuid4()),
            "source_topic": rec.topic,
            "source_partition": rec.partition,
            "source_offset": rec.offset,
            "error_code": error_code,
            "error_message": error_message[:500],
            "quarantined_at": now.isoformat(),
            "raw_payload_b64": base64.b64encode(rec.value).decode("ascii"),
            "correlation_id": corr_id,
            "schema_version": "1.0.0",
        }
        dlq_bytes = self.registry.serialize("fleet.telemetry.dlq", dlq_payload)
        self.bus.produce("fleet.telemetry.dlq", rec.key or "unknown", dlq_bytes)
        self.metrics.dlq_messages_total.labels(topic=rec.topic, error_code=error_code).inc()
        return {
            "dlq_id": dlq_payload["dlq_id"],
            "source_topic": rec.topic,
            "source_partition": rec.partition,
            "source_offset": rec.offset,
            "error_code": error_code,
            "error_message": dlq_payload["error_message"],
            "quarantined_at": now.replace(tzinfo=None),
            "raw_payload_b64": dlq_payload["raw_payload_b64"],
            "correlation_id": corr_id,
        }

    def ingest_records(
        self,
        topic: str,
        records: list[TransportRecord],
        *,
        replay_batch_id: str | None = None,
    ) -> dict[str, int]:
        """Validate and commit a batch of TransportRecords into their Bronze Iceberg table."""
        _, corr_id = new_trace_context()
        valid_rows: list[dict[str, Any]] = []
        dlq_rows: list[dict[str, Any]] = []

        for rec in records:
            try:
                _, decoded = self.registry.deserialize(rec.value, expected_topic=topic)
            except SchemaValidationError as exc:
                dlq_rows.append(
                    self._quarantine_record(rec, "AVRO_SCHEMA_INVALID", str(exc), corr_id)
                )
                continue

            if topic == "fleet.telemetry.raw":
                loc = decoded.get("location") or {}
                pl = decoded["payload"]
                valid_rows.append(
                    {
                        "event_id": decoded["event_id"],
                        "vehicle_id": decoded["vehicle_id"],
                        "trip_id": decoded.get("trip_id"),
                        "event_type": decoded["event_type"],
                        "event_time": _parse_utc(decoded["event_time"]),
                        "ingest_time": _parse_utc(decoded.get("ingest_time")),
                        "sequence_number": int(decoded["sequence_number"]),
                        "schema_version": decoded.get("schema_version", "1.0.0"),
                        "source_id": decoded["source_id"],
                        "correlation_id": decoded["correlation_id"],
                        "trace_id": decoded["trace_id"],
                        "latitude": loc.get("latitude"),
                        "longitude": loc.get("longitude"),
                        "heading_deg": loc.get("heading_deg"),
                        "vehicle_state": pl["vehicle_state"],
                        "region": pl["region"],
                        "vehicle_model": pl["vehicle_model"],
                        "software_version": pl["software_version"],
                        "speed_mps": float(pl["speed_mps"]),
                        "acceleration_mps2": float(pl["acceleration_mps2"]),
                        "braking_force_pct": float(pl["braking_force_pct"]),
                        "battery_soc_pct": float(pl["battery_soc_pct"]),
                        "battery_temp_c": float(pl["battery_temp_c"]),
                        "motor_temp_c": float(pl["motor_temp_c"]),
                        "odometer_km": float(pl["odometer_km"]),
                        "fault_codes_csv": ",".join(pl.get("fault_codes") or []),
                        "sensor_health_score": float(pl["sensor_health_score"]),
                        "validation_status": "VALID_AVRO",
                        "replay_batch_id": replay_batch_id,
                        "source_topic": rec.topic,
                        "source_partition": rec.partition,
                        "source_offset": rec.offset,
                        "metadata_json": json.dumps(decoded.get("metadata") or {}),
                    }
                )
            elif topic == "fleet.trips.raw":
                valid_rows.append(
                    {
                        "event_id": decoded["event_id"],
                        "vehicle_id": decoded["vehicle_id"],
                        "trip_id": decoded["trip_id"],
                        "event_type": decoded["event_type"],
                        "event_time": _parse_utc(decoded["event_time"]),
                        "ingest_time": _parse_utc(decoded.get("ingest_time")),
                        "sequence_number": int(decoded["sequence_number"]),
                        "schema_version": decoded.get("schema_version", "1.0.0"),
                        "latitude": float(decoded["latitude"]),
                        "longitude": float(decoded["longitude"]),
                        "region": decoded["region"],
                        "software_version": decoded["software_version"],
                        "odometer_km": float(decoded["odometer_km"]),
                        "battery_soc_pct": float(decoded["battery_soc_pct"]),
                        "route_id": decoded["route_id"],
                        "replay_batch_id": replay_batch_id,
                    }
                )
            elif topic == "fleet.autonomy.raw":
                valid_rows.append(
                    {
                        "event_id": decoded["event_id"],
                        "vehicle_id": decoded["vehicle_id"],
                        "trip_id": decoded.get("trip_id"),
                        "event_type": decoded["event_type"],
                        "event_time": _parse_utc(decoded["event_time"]),
                        "ingest_time": _parse_utc(decoded.get("ingest_time")),
                        "latitude": float(decoded["latitude"]),
                        "longitude": float(decoded["longitude"]),
                        "region": decoded["region"],
                        "software_version": decoded["software_version"],
                        "disengagement_reason": decoded["disengagement_reason"],
                        "autonomy_mode_before": decoded["autonomy_mode_before"],
                        "takeover_latency_ms": int(decoded["takeover_latency_ms"]),
                        "synthetic_flag": bool(decoded.get("synthetic_flag", True)),
                    }
                )
            elif topic == "fleet.anomalies.detected":
                valid_rows.append(
                    {
                        "anomaly_id": decoded["anomaly_id"],
                        "vehicle_id": decoded["vehicle_id"],
                        "trip_id": decoded.get("trip_id"),
                        "region": decoded["region"],
                        "software_version": decoded["software_version"],
                        "rule_id": decoded["rule_id"],
                        "rule_version": decoded["rule_version"],
                        "severity": decoded["severity"],
                        "event_time": _parse_utc(decoded["event_time"]),
                        "detection_time": _parse_utc(decoded["detection_time"]),
                        "latitude": decoded.get("latitude"),
                        "longitude": decoded.get("longitude"),
                        "h3_cell_res6": decoded.get("h3_cell_res6"),
                        "evidence_json": decoded["evidence_json"],
                        "source_event_ids_csv": ",".join(decoded.get("source_event_ids") or []),
                        "correlation_id": decoded["correlation_id"],
                        "trace_id": decoded["trace_id"],
                    }
                )

        topic_to_table = {
            "fleet.telemetry.raw": "bronze.telemetry_raw",
            "fleet.trips.raw": "bronze.trips_raw",
            "fleet.autonomy.raw": "bronze.autonomy_raw",
            "fleet.anomalies.detected": "bronze.anomalies_stream_raw",
        }
        target_table = topic_to_table[topic]
        committed = 0
        if valid_rows:
            committed = self.lakehouse.append_rows(target_table, valid_rows)
            self.metrics.events_ingested_bronze_total.labels(table=target_table).inc(committed)
        if dlq_rows:
            self.lakehouse.append_rows("bronze.telemetry_dlq", dlq_rows)
        return {"committed": committed, "quarantined_dlq": len(dlq_rows)}

    def drain_all_topics(self, max_records_per_topic: int = 200_000) -> dict[str, Any]:
        """Drain all configured bronze-bound topics and commit offsets only after Iceberg commit."""
        t0 = time.perf_counter()
        run_id = str(uuid.uuid4())
        trace_id, corr_id = new_trace_context()
        topics = [
            "fleet.telemetry.raw",
            "fleet.trips.raw",
            "fleet.autonomy.raw",
            "fleet.anomalies.detected",
        ]
        self.lineage.emit(
            event_type="START",
            job_name="bronze_durable_ingestion",
            run_id=run_id,
            inputs=[{"name": t} for t in topics],
            outputs=[
                {"name": "bronze.telemetry_raw"},
                {"name": "bronze.trips_raw"},
                {"name": "bronze.autonomy_raw"},
                {"name": "bronze.anomalies_stream_raw"},
                {"name": "bronze.telemetry_dlq"},
            ],
        )

        results: dict[str, dict[str, int]] = {}
        try:
            for topic in topics:
                total_committed = 0
                total_dlq = 0
                remaining = max_records_per_topic
                while remaining > 0:
                    batch = self.bus.consume_batch(
                        topic,
                        self.CONSUMER_GROUP,
                        max_records=min(25_000, remaining),
                        auto_commit=False,
                    )
                    if not batch:
                        break
                    outcome = self.ingest_records(topic, batch)
                    total_committed += outcome["committed"]
                    total_dlq += outcome["quarantined_dlq"]
                    # Commit consumer offsets ONLY AFTER Iceberg snapshot commit succeeded
                    part_offsets: dict[int, int] = {}
                    for r in batch:
                        part_offsets[r.partition] = max(
                            part_offsets.get(r.partition, 0), r.offset + 1
                        )
                    self.bus.commit_offsets(topic, self.CONSUMER_GROUP, part_offsets)
                    remaining -= len(batch)
                lag = self.bus.get_consumer_lag(topic, self.CONSUMER_GROUP)
                self.metrics.consumer_lag_records.labels(
                    consumer_group=self.CONSUMER_GROUP, topic=topic
                ).set(lag["total_lag"])
                results[topic] = {
                    "committed": total_committed,
                    "quarantined_dlq": total_dlq,
                    "remaining_lag": int(lag["total_lag"]),
                }

            duration_s = round(time.perf_counter() - t0, 4)
            self.lineage.emit(
                event_type="COMPLETE",
                job_name="bronze_durable_ingestion",
                run_id=run_id,
                inputs=[{"name": t} for t in topics],
                outputs=[
                    {
                        "name": "bronze.telemetry_raw",
                        "row_count": results["fleet.telemetry.raw"]["committed"],
                    },
                    {
                        "name": "bronze.telemetry_dlq",
                        "row_count": sum(v["quarantined_dlq"] for v in results.values()),
                    },
                ],
            )
            self.logger.info(
                "ingestion.drain_complete",
                "Durable Bronze ingestion completed",
                trace_id=trace_id,
                correlation_id=corr_id,
                pipeline_run_id=run_id,
                context={"duration_seconds": duration_s, "topics": results},
            )
            return {
                "run_id": run_id,
                "duration_seconds": duration_s,
                "topics": results,
            }
        except Exception as exc:
            self.lineage.emit(
                event_type="FAIL",
                job_name="bronze_durable_ingestion",
                run_id=run_id,
                run_facets={"errorMessage": {"message": str(exc)}},
            )
            raise

    def replay_time_window(
        self,
        topic: str = "fleet.telemetry.raw",
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        replay_batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Bounded replay into Bronze with explicit replay_batch_id provenance.

        Does not alter consumer group offsets. Downstream Silver deduplication guarantees
        that replaying an already-ingested slice yields identical Gold business aggregates.
        """
        batch_id = replay_batch_id or f"replay-{uuid.uuid4().hex[:10]}"
        records = self.bus.read_range(topic, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms)
        outcome = self.ingest_records(topic, records, replay_batch_id=batch_id)
        self.logger.audit(
            "audit.replay_triggered",
            f"Replayed {len(records)} records from {topic} with replay_batch_id={batch_id}",
            context={
                "topic": topic,
                "replay_batch_id": batch_id,
                "records_read": len(records),
                **outcome,
            },
        )
        return {
            "topic": topic,
            "replay_batch_id": batch_id,
            "records_read": len(records),
            **outcome,
        }
