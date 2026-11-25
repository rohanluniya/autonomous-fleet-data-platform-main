"""Real-time event-time streaming rules and stateful processor."""

from fleet_platform.streaming.rules import (
    RULE_CATALOG,
    VehicleStreamingState,
    evaluate_telemetry_rules,
)
from fleet_platform.streaming.state_engine import StatefulStreamEngine, StreamEngineConfig

__all__ = [
    "RULE_CATALOG",
    "StatefulStreamEngine",
    "StreamEngineConfig",
    "VehicleStreamingState",
    "evaluate_telemetry_rules",
]
