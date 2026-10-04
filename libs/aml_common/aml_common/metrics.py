"""Prometheus metrics for every service (exposed on each service's /metrics, scraped by Prometheus).

Defined as classes taking a registry so tests can use an isolated one. Metric names are the contract
with observability/ (dashboards, alert rules): tests/observability verifies no name drifts.
"""
from __future__ import annotations

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

HOTPATH_BUCKETS = (.001, .0025, .005, .01, .02, .035, .05, .075, .1, .15, .25, .5, 1, 2.5)
E2E_BUCKETS = (.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300)
SCORE_BUCKETS = (.1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0)
FLUSH_BUCKETS = (.001, .005, .01, .025, .05, .1, .25, .5, 1, 5)
CYCLE_BUCKETS = (.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120)


class ProcessorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        r = registry
        self.processed = Counter("aml_txn_processed", "Transactions scored, by verdict.", ["verdict"], registry=r)
        self.alerts = Counter("aml_alerts", "REVIEW/BLOCK decisions by best-matching typology.",
                              ["verdict", "typology"], registry=r)
        self.indicator = Counter("aml_indicator_fired", "Risk indicators fired.", ["indicator"], registry=r)
        self.hotpath = Histogram("aml_hotpath_seconds", "Time inside the hot path per transaction.",
                                 buckets=HOTPATH_BUCKETS, registry=r)
        self.e2e = Histogram("aml_e2e_seconds", "Event time to verdict (includes queueing).",
                             buckets=E2E_BUCKETS, registry=r)
        self.stage = Histogram("aml_stage_seconds", "Hot-path latency by stage.", ["stage"],
                               buckets=HOTPATH_BUCKETS, registry=r)
        self.risk = Histogram("aml_risk_score", "Distribution of risk scores.", buckets=SCORE_BUCKETS, registry=r)
        self.dlq = Counter("aml_dlq", "Messages sent to the dead-letter topic.", registry=r)
        self.pg_flush = Histogram("aml_pg_flush_seconds", "Postgres batch write latency.",
                                  buckets=FLUSH_BUCKETS, registry=r)
        self.pg_errors = Counter("aml_pg_errors", "Failed Postgres batch writes.", registry=r)
        self.lag = Gauge("aml_consumer_lag", "Messages behind the head of transactions.raw.",
                         ["partition"], registry=r)


class AnalyticsMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        r = registry
        self.stage = Gauge("aml_analytics_stage_seconds", "Duration of each stage in the last cycle.",
                           ["stage"], registry=r)
        self.cycle = Histogram("aml_analytics_cycle_seconds", "Total analytics cycle duration.",
                               buckets=CYCLE_BUCKETS, registry=r)
        self.runs = Counter("aml_analytics_runs", "Analytics cycles by engine and outcome.",
                            ["engine", "status"], registry=r)
        self.edges = Gauge("aml_analytics_edges", "Material flow edges in the last window.", registry=r)
        self.accounts = Gauge("aml_analytics_accounts", "Accounts in the analytics graph.", registry=r)
        self.clusters = Gauge("aml_analytics_clusters", "Clusters found in the last cycle.", registry=r)
        self.flagged = Gauge("aml_analytics_flagged_accounts", "Accounts carrying a network flag.", registry=r)
        self.flags = Gauge("aml_analytics_flags", "Accounts per network flag kind.", ["kind"], registry=r)
        self.network_alerts = Counter("aml_network_alerts", "Cluster alerts published.", registry=r)
        self.pruned = Counter("aml_edges_pruned", "Flow edges deleted by retention.", registry=r)
        self.gds_fallbacks = Counter("aml_analytics_gds_fallbacks", "GDS failures that fell back to networkx.",
                                     registry=r)
        self.last_success = Gauge("aml_analytics_last_success_timestamp_seconds",
                                  "Unix time of the last successful cycle.", registry=r)


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        r = registry
        self.txns = Counter("aml_sim_txns", "Simulated transactions emitted.", ["kind"], registry=r)
        self.scenarios = Counter("aml_sim_scenarios", "Laundering scenarios started.", ["typology"], registry=r)
