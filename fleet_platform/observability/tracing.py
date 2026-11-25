"""OpenTelemetry trace context initialization and span helpers."""

from __future__ import annotations

import uuid

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

_CONFIGURED = False


def get_tracer(service_name: str = "autonomous-fleet-platform") -> trace.Tracer:
    global _CONFIGURED
    if not _CONFIGURED:
        provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        trace.set_tracer_provider(provider)
        _CONFIGURED = True
    return trace.get_tracer(service_name)


def new_trace_context() -> tuple[str, str]:
    """Return a fresh (trace_id, correlation_id) pair."""
    return uuid.uuid4().hex, uuid.uuid4().hex[:16]
