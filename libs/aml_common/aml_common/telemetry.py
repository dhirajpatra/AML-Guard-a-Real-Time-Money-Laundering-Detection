"""Tracing (OpenTelemetry -> OTLP -> collector -> Tempo), trace propagation over Kafka headers,
the /metrics endpoint, and the hot-path instrumentation helper.

Tracing is opt-in (OTEL_ENABLED=true). When it is off the tracer is a no-op, so call sites stay unchanged.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from prometheus_client import start_http_server

from .metrics import ProcessorMetrics
from .models import Decision

log = logging.getLogger("telemetry")


def setup_tracing(service_name: str, enabled: bool, endpoint: str, sample_ratio: float = 1.0) -> bool:
    if not enabled:
        return False
    provider = TracerProvider(
        resource=Resource.create({"service.name": service_name, "service.namespace": "aml-guard"}),
        sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)
    log.info("tracing enabled", extra={"ctx": {"endpoint": endpoint, "sample_ratio": sample_ratio}})
    return True


def start_metrics_server(port: int) -> None:
    try:
        start_http_server(port)
        log.info("metrics endpoint up", extra={"ctx": {"port": port}})
    except OSError as exc:
        log.warning("metrics endpoint unavailable on :%s (%s)", port, exc)


def inject_headers() -> list[tuple[str, bytes]]:
    """W3C trace-context headers for the current span (empty list if there is none)."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(k, v.encode()) for k, v in carrier.items()]


def extract_context(headers) -> otel_context.Context | None:
    if not headers:
        return None
    carrier = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in headers if v is not None}
    return propagate.extract(carrier)


class NullObs:
    """Used by tests and when instrumentation is not wanted."""

    @contextmanager
    def stage(self, name: str):
        yield

    def record(self, d: Decision) -> None:
        return None


class ProcessorObs:
    """Times each hot-path stage (histogram + span) and records per-decision metrics."""

    def __init__(self, metrics: ProcessorMetrics, tracer: trace.Tracer | None = None) -> None:
        self.m = metrics
        self.tracer = tracer or trace.get_tracer("aml.hotpath")

    @contextmanager
    def stage(self, name: str):
        t0 = time.perf_counter()
        with self.tracer.start_as_current_span(f"hotpath.{name}"):
            try:
                yield
            finally:
                self.m.stage.labels(name).observe(time.perf_counter() - t0)

    def record(self, d: Decision) -> None:
        m = self.m
        m.processed.labels(d.verdict).inc()
        m.hotpath.observe(d.processing_ms / 1000.0)
        m.e2e.observe(d.e2e_ms / 1000.0)
        m.risk.observe(d.risk_score)
        for ind in d.indicators:
            m.indicator.labels(ind).inc()
        if d.verdict != "PASS":
            m.alerts.labels(d.verdict, d.typology or "none").inc()
