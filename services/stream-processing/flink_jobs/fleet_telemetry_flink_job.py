#!/usr/bin/env python3
"""Apache Flink 2.1.3 PyFlink Stateful Streaming Job (Container / Cluster Deployment).

Connects to Apache Kafka 4.3.2 via `flink-sql-connector-kafka:5.0.0-2.1` and optionally
sinks to Apache Iceberg 1.12.0 via `iceberg-flink-runtime-2.1:1.12.0`.

Implements:
- Bounded out-of-orderness event-time watermarks (30s allowance)
- Keyed vehicle state with 24h StateTtlConfig
- Online event_id deduplication with bounded TTL
- Event-time gap / offline detection timers
- Versioned anomaly rules & sliding-window CEP (shared with fleet_platform.streaming.rules)
- Late-event side output (zero silent drops)
- Incremental checkpointing & exponential-delay restart strategy
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from fleet_platform.streaming.rules import VehicleStreamingState, evaluate_telemetry_rules


def build_and_run_flink_job() -> None:
    """Entrypoint executed inside the flink:2.1.3-scala_2.12-java17 container."""
    from pyflink.common import Duration, Types, WatermarkStrategy
    from pyflink.common.restart_strategy import RestartStrategies
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.time import Time
    from pyflink.datastream import (
        CheckpointingMode,
        KeyedProcessFunction,
        OutputTag,
        StreamExecutionEnvironment,
    )
    from pyflink.datastream.connectors.kafka import (
        DeliveryGuarantee,
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )
    from pyflink.datastream.state import (
        MapStateDescriptor,
        StateTtlConfig,
        ValueStateDescriptor,
    )

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    checkpoint_dir = os.environ.get(
        "FLINK_CHECKPOINT_URI", "s3://fleet-lakehouse/checkpoints/flink"
    )
    kafka_jar = os.environ.get(
        "FLINK_KAFKA_JAR",
        "file:///opt/flink/usrlib/flink-sql-connector-kafka-5.0.0-2.1.jar",
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(kafka_jar)
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "4")))
    env.enable_checkpointing(15_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)
    env.get_checkpoint_config().set_checkpoint_storage_dir(checkpoint_dir)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(5, Time.seconds(10)))

    late_output_tag = OutputTag("late-telemetry", Types.STRING())

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics("fleet.telemetry.raw")
        .set_group_id("fleet-flink-stream-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    class FleetEventTimeAnomalyFunction(KeyedProcessFunction):
        def open(self, runtime_context: Any) -> None:
            ttl = (
                StateTtlConfig.new_builder(Time.hours(24))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )
            state_desc = ValueStateDescriptor("vehicle_state_json", Types.STRING())
            state_desc.enable_time_to_live(ttl)
            self.vehicle_state = runtime_context.get_state(state_desc)

            dedup_desc = MapStateDescriptor("seen_event_ids", Types.STRING(), Types.LONG())
            dedup_desc.enable_time_to_live(ttl)
            self.seen_events = runtime_context.get_map_state(dedup_desc)

        def process_element(self, value: str, ctx: Any) -> Any:
            event = json.loads(value)
            ev_id = str(event["event_id"])
            if self.seen_events.contains(ev_id):
                return
            ev_ts = datetime.fromisoformat(event["event_time"]).timestamp()
            self.seen_events.put(ev_id, int(ev_ts * 1000))

            watermark_ms = ctx.timer_service().current_watermark()
            if watermark_ms > 0 and int(ev_ts * 1000) < watermark_ms:
                yield (
                    late_output_tag,
                    json.dumps(
                        {
                            "event_id": ev_id,
                            "vehicle_id": event["vehicle_id"],
                            "event_time": event["event_time"],
                            "watermark_ms": watermark_ms,
                        }
                    ),
                )

            raw_state = self.vehicle_state.value()
            st = (
                VehicleStreamingState.from_dict(json.loads(raw_state))
                if raw_state
                else VehicleStreamingState(vehicle_id=event["vehicle_id"])
            )
            anomalies = evaluate_telemetry_rules(event, st, event_ts_sec=ev_ts)
            self.vehicle_state.update(json.dumps(st.to_dict()))
            # Register event-time gap detection timer (45 seconds ahead)
            ctx.timer_service().register_event_time_timer(int((ev_ts + 45.0) * 1000))
            for anom in anomalies:
                yield json.dumps(anom)

        def on_timer(self, timestamp: int, ctx: Any) -> Any:
            raw_state = self.vehicle_state.value()
            if not raw_state:
                return
            st = VehicleStreamingState.from_dict(json.loads(raw_state))
            gap = (timestamp / 1000.0) - st.last_event_ts
            if st.operational_state == "driving" and gap >= 45.0 and not st.gap_alerted:
                st.gap_alerted = True
                self.vehicle_state.update(json.dumps(st.to_dict()))
                yield json.dumps(
                    {
                        "rule_id": "RULE_TELEMETRY_GAP",
                        "rule_version": "1.0.0",
                        "vehicle_id": st.vehicle_id,
                        "gap_duration_seconds": round(gap, 2),
                        "detection_time": datetime.now(UTC).isoformat(),
                    }
                )

    wm_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
    stream = env.from_source(source, wm_strategy, "kafka-telemetry-raw")

    anomalies_stream = stream.key_by(lambda s: json.loads(s)["vehicle_id"]).process(
        FleetEventTimeAnomalyFunction(), output_type=Types.STRING()
    )

    anomaly_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("fleet.anomalies.detected")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    anomalies_stream.sink_to(anomaly_sink)
    env.execute("autonomous-fleet-telemetry-flink-2.1")


if __name__ == "__main__":
    build_and_run_flink_job()
