"""Transport factory choosing between Apache Kafka and local partitioned WAL."""

from __future__ import annotations

from fleet_platform.config import PlatformConfig
from fleet_platform.transport.base import EventBus
from fleet_platform.transport.kafka_transport import KafkaEventBus
from fleet_platform.transport.local_wal import LocalWalEventBus


def create_event_bus(config: PlatformConfig) -> EventBus:
    if config.transport_mode == "kafka":
        return KafkaEventBus(bootstrap_servers=config.kafka_bootstrap_servers)
    return LocalWalEventBus(root_dir=config.wal_dir)
