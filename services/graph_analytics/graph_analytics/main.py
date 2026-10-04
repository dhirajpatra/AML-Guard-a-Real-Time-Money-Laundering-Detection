"""Cold path: periodic graph analytics -> network flags for the hot path + :Cluster nodes + edge pruning.

    python -m graph_analytics.main            # run forever, every NET_INTERVAL_S
    python -m graph_analytics.main --once     # one cycle, print a summary, exit
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
import uuid

import redis
from confluent_kafka import Producer
from neo4j import GraphDatabase
from opentelemetry import trace
from prometheus_client import REGISTRY, CollectorRegistry

from aml_common.logging_setup import setup_logging
from aml_common.metrics import AnalyticsMetrics
from aml_common.telemetry import setup_tracing, start_metrics_server

from .engines import GdsEngine, NetworkxEngine
from .netscore import AlgoResult, NetConfig, build_features, cluster_alerts
from .settings import AnalyticsSettings
from .store import (fetch_edges, graph_time, prune_edges, publish_cluster_alerts, write_neo4j,
                    write_redis)

log = logging.getLogger("analytics")
tracer = trace.get_tracer("aml.analytics")


class Analyzer:
    def __init__(self, s: AnalyticsSettings, driver, producer=None, metrics: AnalyticsMetrics | None = None) -> None:
        self.s, self.driver, self.producer = s, driver, producer
        self.metrics = metrics or AnalyticsMetrics(CollectorRegistry())
        self.gds = GdsEngine(driver, s.net_max_cluster)
        self.nx = NetworkxEngine(s.net_max_cluster)
        self.cycle_no = 0
        self.gds_broken = False

    def analyze(self, edges, since: float) -> tuple[AlgoResult, str]:
        mode = self.s.net_engine
        if mode in ("gds", "auto") and (not self.gds_broken or self.cycle_no % 10 == 0):
            try:
                res = self.gds.analyze(edges, since, self.s.net_min_amount)
                self.gds_broken = False
                return res, "gds"
            except Exception as exc:  # noqa: BLE001
                if mode == "gds":
                    raise
                self.gds_broken = True
                self.metrics.gds_fallbacks.inc()
                log.warning("GDS analysis failed, falling back to networkx: %s", str(exc)[:300])
        return self.nx.analyze(edges), "networkx"

    def cycle(self, rds) -> dict:
        s, m = self.s, self.metrics
        self.cycle_no += 1
        t0 = time.perf_counter()
        with tracer.start_as_current_span("analytics.cycle") as span:
            now = graph_time(self.driver)
            if now is None:
                return {"status": "empty graph"}
            since = now - s.net_window_s
            with tracer.start_as_current_span("analytics.fetch_edges"):
                edges = fetch_edges(self.driver, since, s.net_min_amount)
            t1 = time.perf_counter()
            with tracer.start_as_current_span("analytics.algorithms") as sp:
                res, engine = self.analyze(edges, since)
                sp.set_attribute("aml.engine", engine)
            t2 = time.perf_counter()
            cfg = NetConfig(s.net_tight_size, s.net_tight_ratio, s.net_scc_min, s.net_hub_factor,
                            s.net_hub_min_indegree)
            with tracer.start_as_current_span("analytics.score_and_write"):
                feats, clusters = build_features(edges, res, cfg)
                flagged = write_redis(rds, feats, now, s.net_ttl_s)
                write_neo4j(self.driver, uuid.uuid4().hex[:12], feats, clusters, s.cluster_min_size)
                alerts_sent = 0
                if self.producer is not None:
                    alerts_sent = publish_cluster_alerts(
                        rds, self.producer, s.topic_network_alerts,
                        cluster_alerts(feats, clusters, now, s.cluster_min_size))
                    self.producer.flush(5)
            t3 = time.perf_counter()
            with tracer.start_as_current_span("analytics.prune"):
                pruned = prune_edges(self.driver, now - s.edge_retention_s, s.prune_batch)
            t4 = time.perf_counter()
            span.set_attributes({"aml.edges": len(edges), "aml.flagged": flagged, "aml.engine": engine})

        flags = {"circular": sum(f.circular for f in feats.values()), "tight": sum(f.tight for f in feats.values()),
                 "hub": sum(f.hub for f in feats.values())}
        stages = {"fetch": t1 - t0, "analyze": t2 - t1, "write": t3 - t2, "prune": t4 - t3}
        for k, v in stages.items():
            m.stage.labels(k).set(v)
        m.cycle.observe(t4 - t0)
        m.runs.labels(engine, "ok").inc()
        m.edges.set(len(edges)); m.accounts.set(len(feats)); m.clusters.set(len(clusters)); m.flagged.set(flagged)
        for k, v in flags.items():
            m.flags.labels(k).set(v)
        m.network_alerts.inc(alerts_sent); m.pruned.inc(pruned)
        m.last_success.set(time.time())
        big = [c for c in clusters.values() if len(c.members) >= s.cluster_min_size]
        return {
            "engine": engine, "edges": len(edges), "accounts": len(feats), "clusters": len(clusters),
            "clusters_ge_min": len(big), "flagged_accounts": flagged, **flags,
            "network_alerts": alerts_sent, "pruned_edges": pruned,
            "ms": {**{k: round(v * 1000) for k, v in stages.items()}, "total": round((t4 - t0) * 1000)},
        }


def run() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    s = AnalyticsSettings()
    setup_logging(s.log_level, "graph-analytics")
    setup_tracing("graph-analytics", s.otel_enabled, s.otel_exporter_otlp_endpoint, s.otel_trace_sample_ratio)
    if not args.once:
        start_metrics_server(s.metrics_port)
    metrics = AnalyticsMetrics(REGISTRY)

    driver = GraphDatabase.driver(s.neo4j_uri, auth=(s.neo4j_user, s.neo4j_password))
    for i in range(30):
        try:
            driver.verify_connectivity()
            break
        except Exception as exc:  # noqa: BLE001
            log.warning("neo4j not ready (%s/30): %s", i + 1, exc)
            time.sleep(2)
    rds = redis.Redis.from_url(s.redis_url, decode_responses=True)
    rds.ping()
    producer = Producer({"bootstrap.servers": s.kafka_bootstrap, "client.id": "aml-graph-analytics"})
    az = Analyzer(s, driver, producer, metrics)

    stop = {"flag": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.__setitem__("flag", True))

    log.info("graph analytics started", extra={"ctx": {"engine": s.net_engine, "interval_s": s.net_interval_s,
                                                         "window_s": s.net_window_s}})
    while not stop["flag"]:
        started = time.monotonic()
        try:
            stats = az.cycle(rds)
            log.info("analytics cycle", extra={"ctx": stats})
            if args.once:
                print(stats)
        except Exception:  # noqa: BLE001
            metrics.runs.labels(s.net_engine, "error").inc()
            log.exception("analytics cycle failed")
            if args.once:
                raise
        if args.once:
            break
        while not stop["flag"] and time.monotonic() - started < s.net_interval_s:
            time.sleep(0.5)
    driver.close()


if __name__ == "__main__":
    run()
