"""Observability primitives: structured JSON logs, Prometheus metrics, OTel tracing, OpenLineage."""

from fleet_platform.observability.lineage import OpenLineageEmitter
from fleet_platform.observability.logging import StructuredLogger, get_logger
from fleet_platform.observability.metrics import PlatformMetrics, get_metrics
from fleet_platform.observability.tracing import get_tracer, new_trace_context

__all__ = [
    "OpenLineageEmitter",
    "PlatformMetrics",
    "StructuredLogger",
    "get_logger",
    "get_metrics",
    "get_tracer",
    "new_trace_context",
]
