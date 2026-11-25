"""Structured JSON logging with mandatory operational fields and secret scrubbing.

Every log record includes:
timestamp, service, environment, severity, trace_id, correlation_id,
pipeline_run_id, component, event_type, error_code, retry_count, and context.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

_SENSITIVE_KEY_PATTERN = re.compile(
    r"(token|secret|password|authorization|api_key|credential|cookie|private_key)", re.IGNORECASE
)


def _scrub(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: ("[REDACTED]" if _SENSITIVE_KEY_PATTERN.search(str(k)) else _scrub(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    if isinstance(value, str) and len(value) > 512:
        return value[:512] + "...[TRUNCATED]"
    return value


class StructuredLogger:
    """Thin structured JSON logger writing newline-delimited JSON records."""

    def __init__(
        self,
        service: str,
        component: str,
        environment: str | None = None,
        level: str | None = None,
        log_file: str | None = None,
    ) -> None:
        self.service = service
        self.component = component
        self.environment = environment or os.environ.get("FLEET_ENV", "dev")
        self.level_name = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
        self._level = getattr(logging, self.level_name, logging.INFO)
        self._log_file = log_file

    def emit(
        self,
        severity: str,
        event_type: str,
        message: str,
        *,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        pipeline_run_id: str | None = None,
        error_code: str | None = None,
        retry_count: int = 0,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sev_upper = severity.upper()
        sev_num = getattr(logging, sev_upper, logging.INFO)
        record: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": self.service,
            "environment": self.environment,
            "severity": sev_upper,
            "trace_id": trace_id or uuid.uuid4().hex,
            "correlation_id": correlation_id or uuid.uuid4().hex[:16],
            "pipeline_run_id": pipeline_run_id or "none",
            "component": self.component,
            "event_type": event_type,
            "error_code": error_code or "NONE",
            "retry_count": retry_count,
            "message": message,
            "context": _scrub(context or {}),
        }
        if sev_num >= self._level:
            line = json.dumps(record, separators=(",", ":"), default=str)
            stream = sys.stderr if sev_num >= logging.WARNING else sys.stdout
            print(line, file=stream, flush=True)
            if self._log_file:
                with open(self._log_file, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
        return record

    def info(self, event_type: str, message: str, **kwargs: Any) -> dict[str, Any]:
        return self.emit("INFO", event_type, message, **kwargs)

    def warning(self, event_type: str, message: str, **kwargs: Any) -> dict[str, Any]:
        return self.emit("WARNING", event_type, message, **kwargs)

    def error(self, event_type: str, message: str, **kwargs: Any) -> dict[str, Any]:
        return self.emit("ERROR", event_type, message, **kwargs)

    def audit(self, event_type: str, message: str, **kwargs: Any) -> dict[str, Any]:
        ctx = dict(kwargs.pop("context", None) or {})
        ctx["audit"] = True
        return self.emit("INFO", event_type, message, context=ctx, **kwargs)


def get_logger(
    service: str,
    component: str,
    *,
    environment: str | None = None,
    log_file: str | None = None,
) -> StructuredLogger:
    return StructuredLogger(
        service=service,
        component=component,
        environment=environment,
        log_file=log_file,
    )
