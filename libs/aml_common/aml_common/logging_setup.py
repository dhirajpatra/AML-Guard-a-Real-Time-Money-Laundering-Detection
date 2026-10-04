"""Structured JSON logging (picked up by Loki in Phase 5)."""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

from opentelemetry import trace


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str | None = None) -> None:
        super().__init__()
        self.service = service or os.getenv("SERVICE_NAME", "aml-guard")

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        sc = trace.get_current_span().get_span_context()      # lets Grafana jump from a log line to its trace
        if sc.is_valid:
            payload["trace_id"] = format(sc.trace_id, "032x")
            payload["span_id"] = format(sc.span_id, "016x")
        ctx = getattr(record, "ctx", None)
        if isinstance(ctx, dict):
            payload.update(ctx)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO", service: str | None = None) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
