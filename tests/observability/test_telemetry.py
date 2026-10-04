import json
import logging
from types import SimpleNamespace

import fakeredis
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import CollectorRegistry

from aml_common.llm_tracing import langfuse_handler
from aml_common.logging_setup import JsonFormatter
from aml_common.metrics import ProcessorMetrics
from aml_common.telemetry import ProcessorObs, extract_context, inject_headers
from stream_processor.features import FeatureStore
from stream_processor.ontology import DEFAULT_ONTOLOGY
from stream_processor.pipeline import Pipeline
from stream_processor.rules import RuleConfig
from tests.stream_processor.twin import InMemoryGraph, simulate


def tracer_with_exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


def build(obs):
    pop, events, truth = simulate(seed=5, duration_s=240, n_customers=500)
    pipe = Pipeline(FeatureStore(fakeredis.FakeRedis(decode_responses=True)), InMemoryGraph(pop, ["IR", "KP", "MM"]),
                    DEFAULT_ONTOLOGY, RuleConfig(), obs=obs)
    return pipe, events


def test_stage_and_decision_metrics_are_recorded():
    reg = CollectorRegistry()
    obs = ProcessorObs(ProcessorMetrics(reg))
    pipe, events = build(obs)
    decisions = []
    for t in events:
        d = pipe.process(t, now=t.ts)
        obs.record(d)
        decisions.append(d)
    n = len(decisions)
    for stage in ("redis_features", "graph_flow_store", "graph_profiles", "score"):
        assert reg.get_sample_value("aml_stage_seconds_count", {"stage": stage}) == n
    assert reg.get_sample_value("aml_hotpath_seconds_count") == n
    assert reg.get_sample_value("aml_risk_score_count") == n
    by_verdict = {v: sum(d.verdict == v for d in decisions) for v in ("PASS", "REVIEW", "BLOCK")}
    for v, c in by_verdict.items():
        assert (reg.get_sample_value("aml_txn_processed_total", {"verdict": v}) or 0) == c
    flagged = [d for d in decisions if d.verdict != "PASS"]
    assert flagged, "expected at least one alert in 4 minutes of traffic"
    total_alerts = sum(reg.get_sample_value("aml_alerts_total", {"verdict": d.verdict, "typology": d.typology or "none"})
                       for d in {(x.verdict, x.typology): x for x in flagged}.values())
    assert total_alerts == len(flagged)


def test_stage_spans_are_children_of_the_transaction_span():
    tracer, exporter = tracer_with_exporter()
    obs = ProcessorObs(ProcessorMetrics(CollectorRegistry()), tracer)
    pipe, events = build(obs)
    with tracer.start_as_current_span("process_txn"):
        pipe.process(events[0], now=events[0].ts)
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert set(spans) == {"process_txn", "hotpath.redis_features", "hotpath.graph_flow_store",
                          "hotpath.graph_profiles", "hotpath.score"}
    root = spans["process_txn"]
    assert all(s.parent.span_id == root.context.span_id for n, s in spans.items() if n != "process_txn")
    assert len({s.context.trace_id for s in spans.values()}) == 1


def test_trace_context_survives_kafka_headers():
    tracer, exporter = tracer_with_exporter()
    assert inject_headers() == []                                   # no active span -> nothing to propagate
    with tracer.start_as_current_span("simulator.emit") as producer_span:
        headers = inject_headers()
    assert [k for k, _ in headers] == ["traceparent"] and isinstance(headers[0][1], bytes)
    ctx = extract_context(headers)
    with tracer.start_as_current_span("process_txn", context=ctx) as consumer_span:
        pass
    assert consumer_span.context.trace_id == producer_span.context.trace_id
    assert consumer_span.parent.span_id == producer_span.context.span_id
    assert extract_context(None) is None


def test_logs_carry_trace_ids_only_inside_a_span():
    tracer, _ = tracer_with_exporter()
    fmt = JsonFormatter("stream-processor")
    rec = logging.LogRecord("x", logging.INFO, "f.py", 1, "hello", None, None)
    plain = json.loads(fmt.format(rec))
    assert plain["service"] == "stream-processor" and "trace_id" not in plain
    with tracer.start_as_current_span("s") as span:
        inside = json.loads(fmt.format(rec))
    assert inside["trace_id"] == format(span.context.trace_id, "032x")
    assert inside["span_id"] == format(span.context.span_id, "016x")


def test_langfuse_handler_is_optional():
    assert langfuse_handler(SimpleNamespace(langfuse_public_key="", langfuse_secret_key="",
                                            langfuse_host="http://x")) is None
    keys = SimpleNamespace(langfuse_public_key="pk", langfuse_secret_key="sk", langfuse_host="http://x")
    try:
        import langfuse  # noqa: F401
    except ImportError:
        assert langfuse_handler(keys) is None                       # SDK missing -> agents still run
