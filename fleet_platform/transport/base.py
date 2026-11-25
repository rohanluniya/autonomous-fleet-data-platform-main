"""Topic topology, Murmur2 key partitioning, and transport abstractions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import mmh3


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int
    compression: str = "zstd"
    cleanup_policy: str = "delete"
    replication_factor_prod: int = 3
    replication_factor_local: int = 1
    description: str = ""


TOPIC_SPECS: dict[str, TopicSpec] = {
    "fleet.telemetry.raw": TopicSpec(
        name="fleet.telemetry.raw",
        partitions=12,
        retention_ms=7 * 86_400_000,
        description="Canonical high-frequency vehicle telemetry keyed by vehicle_id",
    ),
    "fleet.gps.raw": TopicSpec(
        name="fleet.gps.raw",
        partitions=12,
        retention_ms=7 * 86_400_000,
        description="High-frequency GPS position updates keyed by vehicle_id",
    ),
    "fleet.diagnostics.raw": TopicSpec(
        name="fleet.diagnostics.raw",
        partitions=6,
        retention_ms=14 * 86_400_000,
        description="Fault codes, thermal alerts, and sensor degradation reports",
    ),
    "fleet.trips.raw": TopicSpec(
        name="fleet.trips.raw",
        partitions=6,
        retention_ms=30 * 86_400_000,
        description="Trip start and trip end boundary events",
    ),
    "fleet.autonomy.raw": TopicSpec(
        name="fleet.autonomy.raw",
        partitions=6,
        retention_ms=30 * 86_400_000,
        description="Synthetic autonomy disengagement events (clearly flagged simulated)",
    ),
    "fleet.anomalies.detected": TopicSpec(
        name="fleet.anomalies.detected",
        partitions=6,
        retention_ms=30 * 86_400_000,
        description="Versioned, explainable anomalies emitted by stream processing",
    ),
    "fleet.vehicle-state": TopicSpec(
        name="fleet.vehicle-state",
        partitions=6,
        retention_ms=30 * 86_400_000,
        cleanup_policy="compact,delete",
        description="Latest keyed operational and connectivity state per vehicle",
    ),
    "fleet.cdc.vehicle-registry": TopicSpec(
        name="fleet.cdc.vehicle-registry",
        partitions=6,
        retention_ms=30 * 86_400_000,
        description="PostgreSQL Debezium CDC events for vehicle metadata and firmware",
    ),
    "fleet.telemetry.dlq": TopicSpec(
        name="fleet.telemetry.dlq",
        partitions=3,
        retention_ms=30 * 86_400_000,
        description="Dead-letter queue for schema rejections and poison messages",
    ),
}


def murmur2_partition(key: str, num_partitions: int) -> int:
    """Deterministic positive partition assignment matching Kafka key-partitioning semantics.

    Guarantees all events with identical vehicle_id land in the same partition,
    preserving per-vehicle append order within that partition while NOT guaranteeing
    global order across partitions or across out-of-order buffered reconnects.
    """
    if num_partitions <= 1:
        return 0
    raw_hash = mmh3.hash(key.encode("utf-8"), seed=0, signed=False)
    return int((raw_hash & 0x7FFFFFFF) % num_partitions)


@dataclass
class TransportRecord:
    topic: str
    partition: int
    offset: int
    key: str
    value: bytes
    timestamp_ms: int
    headers: dict[str, str] = field(default_factory=dict)


class EventBus(ABC):
    """Unified interface for Kafka (production/Docker) and partitioned local WAL fallback."""

    backend_name: str = "abstract"

    @abstractmethod
    def ensure_topics(self) -> None:
        """Provision all configured platform topics."""

    @abstractmethod
    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        *,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> TransportRecord:
        """Append one serialized payload to a topic partition determined by key."""

    @abstractmethod
    def produce_batch(
        self,
        topic: str,
        items: Sequence[tuple[str, bytes, int | None, dict[str, str] | None]],
    ) -> int:
        """High-throughput batched produce returning count written."""

    @abstractmethod
    def consume_batch(
        self,
        topic: str,
        consumer_group: str,
        *,
        max_records: int = 5000,
        auto_commit: bool = False,
    ) -> list[TransportRecord]:
        """Consume up to max_records from committed consumer_group offsets."""

    @abstractmethod
    def commit_offsets(
        self,
        topic: str,
        consumer_group: str,
        partition_offsets: dict[int, int],
    ) -> None:
        """Persist next offset to read per partition for a consumer group."""

    @abstractmethod
    def get_consumer_lag(self, topic: str, consumer_group: str) -> dict[str, Any]:
        """Return total and per-partition consumer lag."""

    @abstractmethod
    def read_range(
        self,
        topic: str,
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        max_records: int = 100_000,
    ) -> list[TransportRecord]:
        """Read a bounded slice of records for deterministic replay without altering group offsets."""

    @abstractmethod
    def topic_stats(self) -> dict[str, dict[str, Any]]:
        """Return partition counts, total records, and byte sizes for all topics."""
