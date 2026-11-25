"""Production Apache Kafka 4.3.2 transport adapter using confluent-kafka (librdkafka).

Configured with acks=all, idempotent producer, ZSTD compression, batching, retries,
per-vehicle key partitioning, AdminClient topic creation, and bounded timestamp replay.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from fleet_platform.transport.base import (
    TOPIC_SPECS,
    EventBus,
    TransportRecord,
    murmur2_partition,
)


class KafkaEventBus(EventBus):
    """Production Apache Kafka transport backed by librdkafka."""

    backend_name = "kafka"

    def __init__(self, bootstrap_servers: str, client_id: str = "fleet-platform") -> None:
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": f"{client_id}-producer",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "zstd",
                "linger.ms": 10,
                "batch.size": 65536,
                "retries": 5,
                "retry.backoff.ms": 200,
                "max.in.flight.requests.per.connection": 5,
            }
        )

    def ensure_topics(self) -> None:
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        existing = set(admin.list_topics(timeout=10).topics.keys())
        new_topics = []
        for topic, spec in TOPIC_SPECS.items():
            if topic not in existing:
                new_topics.append(
                    NewTopic(
                        topic=topic,
                        num_partitions=spec.partitions,
                        replication_factor=spec.replication_factor_local,
                        config={
                            "retention.ms": str(spec.retention_ms),
                            "compression.type": spec.compression,
                            "cleanup.policy": spec.cleanup_policy,
                        },
                    )
                )
        if new_topics:
            futures = admin.create_topics(new_topics)
            for _, fut in futures.items():
                try:
                    fut.result(timeout=15)
                except KafkaException as exc:
                    if "TOPIC_ALREADY_EXISTS" not in str(exc):
                        raise

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        *,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> TransportRecord:
        spec = TOPIC_SPECS[topic]
        part = murmur2_partition(key, spec.partitions)
        ts_ms = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        delivered: dict[str, Any] = {}

        def _on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                delivered["error"] = err
            else:
                delivered["partition"] = msg.partition()
                delivered["offset"] = msg.offset()

        kafka_headers: list[tuple[str, str | bytes | None]] = [
            (k, v.encode("utf-8")) for k, v in (headers or {}).items()
        ]
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            partition=part,
            timestamp=ts_ms,
            headers=kafka_headers,
            on_delivery=_on_delivery,
        )
        self._producer.flush(timeout=10)
        if "error" in delivered:
            raise KafkaException(delivered["error"])
        return TransportRecord(
            topic=topic,
            partition=int(delivered.get("partition", part)),
            offset=int(delivered.get("offset", -1)),
            key=key,
            value=value,
            timestamp_ms=ts_ms,
            headers=headers or {},
        )

    def produce_batch(
        self,
        topic: str,
        items: Sequence[tuple[str, bytes, int | None, dict[str, str] | None]],
    ) -> int:
        spec = TOPIC_SPECS[topic]
        now_ms = int(time.time() * 1000)
        for key, value, ts_opt, hdr_opt in items:
            part = murmur2_partition(key, spec.partitions)
            kafka_headers: list[tuple[str, str | bytes | None]] = [
                (k, v.encode("utf-8")) for k, v in (hdr_opt or {}).items()
            ]
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                partition=part,
                timestamp=ts_opt if ts_opt is not None else now_ms,
                headers=kafka_headers,
            )
            self._producer.poll(0)
        self._producer.flush(timeout=30)
        return len(items)

    def _build_consumer(self, consumer_group: str) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )

    def consume_batch(
        self,
        topic: str,
        consumer_group: str,
        *,
        max_records: int = 5000,
        auto_commit: bool = False,
    ) -> list[TransportRecord]:
        consumer = self._build_consumer(consumer_group)
        consumer.subscribe([topic])
        records: list[TransportRecord] = []
        empty_polls = 0
        try:
            while len(records) < max_records and empty_polls < 3:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    continue
                raw_headers = msg.headers() or []
                hdrs: dict[str, str] = {}
                for item in raw_headers:
                    if isinstance(item, tuple) and len(item) == 2:
                        hk, hv = item
                        hdrs[str(hk)] = hv.decode("utf-8") if isinstance(hv, bytes) else str(hv)
                records.append(
                    TransportRecord(
                        topic=str(msg.topic() or topic),
                        partition=int(msg.partition() or 0),
                        offset=int(msg.offset() or 0),
                        key=(msg.key() or b"").decode("utf-8"),
                        value=msg.value() or b"",
                        timestamp_ms=int(msg.timestamp()[1]),
                        headers=hdrs,
                    )
                )
            if auto_commit and records:
                consumer.commit(asynchronous=False)
        finally:
            consumer.close()
        return records

    def commit_offsets(
        self,
        topic: str,
        consumer_group: str,
        partition_offsets: dict[int, int],
    ) -> None:
        consumer = self._build_consumer(consumer_group)
        try:
            tps = [TopicPartition(topic, int(p), int(off)) for p, off in partition_offsets.items()]
            consumer.commit(offsets=tps, asynchronous=False)
        finally:
            consumer.close()

    def get_consumer_lag(self, topic: str, consumer_group: str) -> dict[str, Any]:
        consumer = self._build_consumer(consumer_group)
        try:
            md = consumer.list_topics(topic, timeout=10)
            parts = list(md.topics[topic].partitions.keys())
            tps = [TopicPartition(topic, p) for p in parts]
            committed = consumer.committed(tps, timeout=10)
            per_part: dict[int, dict[str, int]] = {}
            total_lag = 0
            for tp in committed:
                _, high = consumer.get_watermark_offsets(tp, timeout=10)
                cur = max(0, tp.offset)
                lag = max(0, high - cur)
                per_part[tp.partition] = {"committed": cur, "high_watermark": high, "lag": lag}
                total_lag += lag
            return {
                "topic": topic,
                "consumer_group": consumer_group,
                "backend": self.backend_name,
                "total_lag": total_lag,
                "partitions": per_part,
            }
        finally:
            consumer.close()

    def read_range(
        self,
        topic: str,
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        max_records: int = 100_000,
    ) -> list[TransportRecord]:
        consumer = self._build_consumer(f"replay-{int(time.time() * 1000)}")
        try:
            md = consumer.list_topics(topic, timeout=10)
            parts = list(md.topics[topic].partitions.keys())
            if start_ts_ms is not None:
                search_tps = [TopicPartition(topic, p, start_ts_ms) for p in parts]
                assign_tps = consumer.offsets_for_times(search_tps, timeout=10)
            else:
                assign_tps = [TopicPartition(topic, p, 0) for p in parts]
            consumer.assign([tp for tp in assign_tps if tp.offset >= 0])
            records: list[TransportRecord] = []
            empty_polls = 0
            while len(records) < max_records and empty_polls < 3:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    continue
                ts = int(msg.timestamp()[1])
                if end_ts_ms is not None and ts > end_ts_ms:
                    continue
                records.append(
                    TransportRecord(
                        topic=str(msg.topic() or topic),
                        partition=int(msg.partition() or 0),
                        offset=int(msg.offset() or 0),
                        key=(msg.key() or b"").decode("utf-8"),
                        value=msg.value() or b"",
                        timestamp_ms=ts,
                    )
                )
            return records
        finally:
            consumer.close()

    def topic_stats(self) -> dict[str, dict[str, Any]]:
        consumer = self._build_consumer("fleet-stats-probe")
        try:
            stats: dict[str, dict[str, Any]] = {}
            md = consumer.list_topics(timeout=10)
            for topic in TOPIC_SPECS:
                if topic not in md.topics:
                    continue
                parts = list(md.topics[topic].partitions.keys())
                total_records = 0
                for p in parts:
                    low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
                    total_records += max(0, high - low)
                stats[topic] = {
                    "partitions": len(parts),
                    "records": total_records,
                    "bytes": -1,
                    "backend": self.backend_name,
                }
            return stats
        finally:
            consumer.close()
