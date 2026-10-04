"""Generates the Grafana dashboards (JSON) in ./dashboards.   python observability/grafana/build_dashboards.py

Dashboards are generated so panels stay consistent; tests/observability validates every PromQL expression
and checks that every aml_* metric used is actually defined in aml_common.metrics.
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
PROM, LOKI, TEMPO = ({"type": t, "uid": t} for t in ("prometheus", "loki", "tempo"))


def tgt(expr, legend="", ref="A", **kw):
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": ref, **kw}


def ts(pid, title, targets, x, y, w, h, unit="short", stack=False, desc=""):
    return {"id": pid, "type": "timeseries", "title": title, "description": desc, "datasource": PROM,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "custom": {
                "drawStyle": "line", "lineWidth": 1, "fillOpacity": 15 if stack else 8, "showPoints": "never",
                "stacking": {"mode": "normal" if stack else "none", "group": "A"}}}, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "none"}}}


def stat(pid, title, expr, x, y, w=4, h=4, unit="short", steps=None, desc=""):
    steps = steps or [{"color": "green", "value": None}]
    return {"id": pid, "type": "stat", "title": title, "description": desc, "datasource": PROM,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": [tgt(expr, instant=True)],
            "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}},
                            "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "value", "graphMode": "area", "textMode": "auto"}}


def logs(pid, title, expr, x, y, w, h):
    return {"id": pid, "type": "logs", "title": title, "datasource": LOKI,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [{"datasource": LOKI, "expr": expr, "refId": "A"}],
            "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending",
                        "enableLogDetails": True, "prettifyLogMessage": False}}


def traces(pid, title, query, x, y, w, h):
    return {"id": pid, "type": "table", "title": title, "datasource": TEMPO,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [{"datasource": TEMPO, "queryType": "traceql", "query": query, "limit": 20,
                         "tableType": "traces", "refId": "A"}],
            "description": "Click a trace id to open it; logs of the same trace are one click away."}


def row(pid, title, y):
    return {"id": pid, "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
            "panels": []}


def dash(uid, title, panels, desc=""):
    return {"uid": uid, "title": title, "description": desc, "tags": ["aml-guard"], "timezone": "browser",
            "schemaVersion": 39, "version": 1, "refresh": "10s", "time": {"from": "now-30m", "to": "now"},
            "templating": {"list": []}, "annotations": {"list": []}, "editable": True, "panels": panels,
            "links": [{"type": "dashboards", "tags": ["aml-guard"], "asDropdown": True, "title": "AML-Guard",
                       "includeVars": False, "keepTime": True}]}


def q(quantile, metric, by="le"):
    return f"histogram_quantile({quantile}, sum by ({by}) (rate({metric}[5m])))"


RED, AMBER = {"color": "red", "value": 0.2}, {"color": "orange", "value": 0.1}


def overview():
    p = [row(1, "Throughput and verdicts", 0),
         stat(2, "Transactions / s", "sum(rate(aml_txn_processed_total[1m]))", 0, 1, unit="reqps"),
         stat(3, "Flagged share (5m)",
              'sum(rate(aml_txn_processed_total{verdict!="PASS"}[5m])) / sum(rate(aml_txn_processed_total[5m]))',
              4, 1, unit="percentunit", steps=[{"color": "green", "value": None}, AMBER, RED],
              desc="REVIEW + BLOCK as a share of all scored transactions"),
         stat(4, "Hot-path p99", q(0.99, "aml_hotpath_seconds_bucket"), 8, 1, unit="s",
              steps=[{"color": "green", "value": None}, {"color": "orange", "value": 0.1}, {"color": "red", "value": 0.2}]),
         stat(5, "E2E p95", q(0.95, "aml_e2e_seconds_bucket"), 12, 1, unit="s",
              steps=[{"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 5}]),
         stat(6, "Consumer lag", "sum(aml_consumer_lag)", 16, 1,
              steps=[{"color": "green", "value": None}, {"color": "orange", "value": 100}, {"color": "red", "value": 1000}]),
         stat(7, "Dead-lettered (1h)", "sum(increase(aml_dlq_total[1h]))", 20, 1,
              steps=[{"color": "green", "value": None}, {"color": "red", "value": 1}]),
         ts(8, "Verdicts per second", [tgt("sum by (verdict) (rate(aml_txn_processed_total[1m]))", "{{verdict}}")],
            0, 5, 12, 8, unit="reqps", stack=True),
         ts(9, "Alerts by typology", [tgt("sum by (typology, verdict) (rate(aml_alerts_total[5m]))",
                                          "{{typology}} / {{verdict}}")], 12, 5, 12, 8, unit="reqps", stack=True),
         row(10, "Latency", 13),
         ts(11, "Hot-path latency (processing time)",
            [tgt(q(0.5, "aml_hotpath_seconds_bucket"), "p50", "A"), tgt(q(0.95, "aml_hotpath_seconds_bucket"), "p95", "B"),
             tgt(q(0.99, "aml_hotpath_seconds_bucket"), "p99", "C")], 0, 14, 12, 8, unit="s",
            desc="Time inside the hot path per transaction (Redis + Neo4j + scoring)."),
         ts(12, "Latency by stage (p95)", [tgt(q(0.95, "aml_stage_seconds_bucket", "le, stage"), "{{stage}}")],
            12, 14, 12, 8, unit="s"),
         ts(13, "End-to-end latency (event time to verdict)",
            [tgt(q(0.5, "aml_e2e_seconds_bucket"), "p50", "A"), tgt(q(0.95, "aml_e2e_seconds_bucket"), "p95", "B"),
             tgt(q(0.99, "aml_e2e_seconds_bucket"), "p99", "C")], 0, 22, 12, 8, unit="s",
            desc="Includes queueing: grows when the processor falls behind."),
         ts(14, "Consumer lag by partition", [tgt("aml_consumer_lag", "partition {{partition}}")], 12, 22, 12, 8),
         row(15, "Detection signals", 30),
         ts(16, "Indicators firing", [tgt("sum by (indicator) (rate(aml_indicator_fired_total[5m]))", "{{indicator}}")],
            0, 31, 12, 8, unit="reqps", stack=True),
         ts(17, "Risk score", [tgt("sum(rate(aml_risk_score_sum[5m])) / sum(rate(aml_risk_score_count[5m]))", "mean", "A"),
                               tgt(q(0.95, "aml_risk_score_bucket"), "p95", "B")], 12, 31, 6, 8),
         ts(18, "Simulated traffic", [tgt("sum by (kind) (rate(aml_sim_txns_total[1m]))", "{{kind}}")], 18, 31, 6, 8,
            unit="reqps", desc="Ground-truth view: what the simulator is emitting."),
         row(19, "Persistence", 39),
         ts(20, "Postgres flush latency (p95) and errors",
            [tgt(q(0.95, "aml_pg_flush_seconds_bucket"), "flush p95", "A"),
             tgt("sum(increase(aml_pg_errors_total[5m]))", "errors (5m)", "B")], 0, 40, 12, 7, unit="s"),
         ts(21, "Dead-letter rate", [tgt("sum(rate(aml_dlq_total[5m]))", "dlq / s")], 12, 40, 12, 7, unit="reqps"),
         row(22, "Traces and logs", 47),
         traces(23, "Recent BLOCK decisions (traces)", '{ span.aml.verdict = "BLOCK" }', 0, 48, 12, 9),
         traces(24, "Slowest hot-path spans", '{ name = "process_txn" && duration > 100ms }', 12, 48, 12, 9),
         logs(25, "stream-processor warnings and errors",
              '{service="stream-processor"} | json | level=~"WARNING|ERROR"', 0, 57, 12, 9),
         logs(26, "stream-processor (all, trace ids are clickable)", '{service="stream-processor"}', 12, 57, 12, 9)]
    return dash("aml-overview", "AML-Guard / Overview", p, "Real-time pipeline health, latency and detection signals.")


def coldpath():
    p = [stat(1, "Last cycle", "time() - aml_analytics_last_success_timestamp_seconds", 0, 0, unit="s",
              steps=[{"color": "green", "value": None}, {"color": "orange", "value": 120}, {"color": "red", "value": 300}],
              desc="Seconds since the last successful analytics cycle"),
         stat(2, "Flagged accounts", "aml_analytics_flagged_accounts", 4, 0),
         stat(3, "Clusters", "aml_analytics_clusters", 8, 0),
         stat(4, "Flow edges in window", "aml_analytics_edges", 12, 0),
         stat(5, "Network alerts (1h)", "sum(increase(aml_network_alerts_total[1h]))", 16, 0),
         stat(6, "GDS fallbacks (1h)", "sum(increase(aml_analytics_gds_fallbacks_total[1h]))", 20, 0,
              steps=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]),
         ts(7, "Cycle time by stage", [tgt("aml_analytics_stage_seconds", "{{stage}}")], 0, 4, 12, 8, unit="s", stack=True),
         ts(8, "Cycle duration (p95)", [tgt(q(0.95, "aml_analytics_cycle_seconds_bucket"), "p95")], 12, 4, 12, 8, unit="s"),
         ts(9, "Network flags", [tgt("aml_analytics_flags", "{{kind}}", "A"),
                                 tgt("aml_analytics_flagged_accounts", "any flag", "B")], 0, 12, 12, 8),
         ts(10, "Graph size", [tgt("aml_analytics_edges", "edges", "A"), tgt("aml_analytics_accounts", "accounts", "B"),
                               tgt("aml_analytics_clusters", "clusters", "C")], 12, 12, 12, 8),
         ts(11, "Cycles by engine and outcome",
            [tgt("sum by (engine, status) (increase(aml_analytics_runs_total[10m]))", "{{engine}} / {{status}}")],
            0, 20, 12, 8),
         ts(12, "Network alerts and edge pruning",
            [tgt("sum(increase(aml_network_alerts_total[10m]))", "network alerts (10m)", "A"),
             tgt("sum(increase(aml_edges_pruned_total[10m]))", "edges pruned (10m)", "B")], 12, 20, 12, 8),
         logs(13, "graph-analytics logs", '{service="graph-analytics"}', 0, 28, 24, 9)]
    return dash("aml-coldpath", "AML-Guard / Cold path (graph analytics)", p,
                "Scheduled Neo4j analytics: clusters, loops, hubs, network alerts, retention.")


def infra():
    p = [{"id": 1, "type": "table", "title": "Scrape targets", "datasource": PROM,
          "gridPos": {"x": 0, "y": 0, "w": 24, "h": 7}, "targets": [tgt("up", "", instant=True, format="table")],
          "transformations": [{"id": "organize", "options": {
              "excludeByName": {"Time": True, "__name__": True}, "renameByName": {"Value": "up"}}}],
          "fieldConfig": {"defaults": {"mappings": [{"type": "value", "options": {
              "0": {"text": "DOWN", "color": "red"}, "1": {"text": "UP", "color": "green"}}}]}, "overrides": []}},
         ts(2, "Redis memory", [tgt("redis_memory_used_bytes", "used")], 0, 7, 8, 8, unit="bytes"),
         ts(3, "Redis commands / s", [tgt("rate(redis_commands_processed_total[1m])", "commands")], 8, 7, 8, 8, unit="ops"),
         ts(4, "Redis keys and clients", [tgt("sum(redis_db_keys)", "keys", "A"),
                                          tgt("redis_connected_clients", "clients", "B")], 16, 7, 8, 8),
         ts(5, "Postgres connections", [tgt('pg_stat_database_numbackends{datname="aml"}', "backends")], 0, 15, 8, 8),
         ts(6, "Postgres rows inserted / s", [tgt('rate(pg_stat_database_tup_inserted{datname="aml"}[1m])', "inserted")],
            8, 15, 8, 8),
         ts(7, "Postgres commits / s", [tgt('rate(pg_stat_database_xact_commit{datname="aml"}[1m])', "commits")],
            16, 15, 8, 8),
         ts(8, "Container CPU (cAdvisor, optional)",
            [tgt('sum by (name) (rate(container_cpu_usage_seconds_total{name=~"aml-.*"}[1m]))', "{{name}}")],
            0, 23, 12, 8, desc="Needs `--profile cadvisor` (Linux hosts)."),
         ts(9, "Container memory (cAdvisor, optional)",
            [tgt('sum by (name) (container_memory_working_set_bytes{name=~"aml-.*"})', "{{name}}")],
            12, 23, 12, 8, unit="bytes")]
    return dash("aml-infra", "AML-Guard / Infrastructure", p, "Redis, Postgres, scrape health, container resources.")


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    for d in (overview(), coldpath(), infra()):
        (OUT / f"{d['uid']}.json").write_text(json.dumps(d, indent=2) + "\n")
        print("wrote", d["uid"], len(d["panels"]), "panels")
