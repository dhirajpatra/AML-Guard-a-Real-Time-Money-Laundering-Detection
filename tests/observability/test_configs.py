import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from prometheus_client import CollectorRegistry

from aml_common.metrics import AnalyticsMetrics, ProcessorMetrics, SimulatorMetrics

ROOT = Path(__file__).resolve().parents[2]
OBS = ROOT / "observability"
DASHBOARDS = sorted((OBS / "grafana" / "dashboards").glob("*.json"))


def load_yaml(p):
    return yaml.safe_load(Path(p).read_text())


def prom_exprs():
    out = []
    for f in DASHBOARDS:
        for p in json.loads(f.read_text())["panels"]:
            for t in p.get("targets", []):
                if t.get("datasource", {}).get("type") == "prometheus":
                    out.append((f.name, t["expr"]))
    for g in load_yaml(OBS / "prometheus" / "rules.yml")["groups"]:
        out += [("rules.yml", r["expr"]) for r in g["rules"]]
    return out


def defined_metric_names():
    reg, names = CollectorRegistry(), set()
    for cls in (ProcessorMetrics, AnalyticsMetrics, SimulatorMetrics):
        cls(reg)
    for fam in reg.collect():
        if fam.type == "counter":
            names.add(fam.name + "_total")
        elif fam.type == "histogram":
            names |= {fam.name + s for s in ("_bucket", "_sum", "_count")}
        else:
            names.add(fam.name)
    return names


def test_all_yaml_configs_parse():
    for p in [OBS / "otel" / "collector.yaml", OBS / "tempo" / "tempo.yaml", OBS / "loki" / "loki.yaml",
              OBS / "prometheus" / "prometheus.yml", OBS / "prometheus" / "rules.yml",
              OBS / "grafana" / "provisioning" / "datasources" / "datasources.yaml",
              OBS / "grafana" / "provisioning" / "dashboards" / "dashboards.yaml"]:
        assert isinstance(load_yaml(p), dict), p
    assert (OBS / "alloy" / "config.alloy").read_text().count("{") == (OBS / "alloy" / "config.alloy").read_text().count("}")


def test_collector_pipeline_is_wired():
    c = load_yaml(OBS / "otel" / "collector.yaml")
    pipe = c["service"]["pipelines"]["traces"]
    for kind in ("receivers", "processors", "exporters"):
        for name in pipe[kind]:
            assert name in c[kind], (kind, name)
    assert "tail_sampling" in pipe["processors"]
    keys = {p["name"] for p in c["processors"]["tail_sampling"]["policies"]}
    assert {"alerts", "errors", "slow", "baseline"} <= keys


def test_prometheus_targets_match_compose_and_service_ports():
    compose = load_yaml(ROOT / "docker-compose.yml")["services"]
    cfg = load_yaml(OBS / "prometheus" / "prometheus.yml")
    ports = {"stream-processor": 8001, "simulator": 8002, "graph-analytics": 8003}
    for sc in cfg["scrape_configs"]:
        for sconf in sc["static_configs"]:
            for target in sconf["targets"]:
                host, port = target.split(":")
                if host != "localhost":
                    assert host in compose, f"{target} is not a compose service"
                if host in ports:
                    assert int(port) == ports[host]
    sys.path.insert(0, str(ROOT / "services" / "stream_processor"))
    from stream_processor.settings import ProcSettings
    from simulator.settings import SimSettings
    from graph_analytics.settings import AnalyticsSettings
    assert (ProcSettings().metrics_port, SimSettings().metrics_port, AnalyticsSettings().metrics_port) == (8001, 8002, 8003)


def test_compose_obs_services_mount_existing_files():
    compose = load_yaml(ROOT / "docker-compose.yml")["services"]
    for name, svc in compose.items():
        if "obs" not in svc.get("profiles", []):
            continue
        for vol in svc.get("volumes", []):
            src = vol.split(":")[0]
            if src.startswith("./"):
                assert (ROOT / src).exists(), f"{name}: {src}"


def test_dashboards_are_valid_and_in_sync_with_generator(tmp_path):
    assert len(DASHBOARDS) == 3
    uids = {json.loads(f.read_text())["uid"] for f in DASHBOARDS}
    assert uids == {"aml-overview", "aml-coldpath", "aml-infra"}
    # regenerate into a temp dir and compare: dashboards must never be hand-edited out of sync
    spec = importlib.util.spec_from_file_location("bd", OBS / "grafana" / "build_dashboards.py")
    bd = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bd)
    for d in (bd.overview(), bd.coldpath(), bd.infra()):
        on_disk = json.loads((OBS / "grafana" / "dashboards" / f"{d['uid']}.json").read_text())
        assert on_disk == d, f"{d['uid']} is stale: run `make dashboards`"


def test_datasource_uids_used_by_dashboards_exist():
    known = {d["uid"] for d in load_yaml(OBS / "grafana" / "provisioning" / "datasources" / "datasources.yaml")["datasources"]}
    for f in DASHBOARDS:
        d = json.loads(f.read_text())
        for p in d["panels"]:
            if "datasource" in p:
                assert p["datasource"]["uid"] in known


def test_every_promql_expression_parses():
    promql = pytest.importorskip("promql_parser")
    exprs = prom_exprs()
    assert len(exprs) > 40
    for where, e in exprs:
        promql.parse(e)       # raises on invalid syntax


def test_dashboards_and_rules_only_use_defined_metrics():
    defined = defined_metric_names()
    used = {m for _, e in prom_exprs() for m in re.findall(r"\baml_[a-z0-9_]+", e)}
    assert used, "no aml_* metrics referenced?"
    missing = used - defined
    assert not missing, f"dashboards/rules reference undefined metrics: {sorted(missing)}"
