"""Digital twin of the hot path's graph layer.

InMemoryGraph implements the same interface and the same flow-tracing semantics as
stream_processor.graph.GraphStore (flow-conserving paths, relationship-unique DFS), so the
rules/thresholds can be exercised end to end without a Neo4j server. It does NOT test the Cypher
text itself - that is covered by the live evaluator (`make eval`).
"""
from __future__ import annotations

import heapq
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from aml_common.models import Transaction
from simulator.population import Population, build_population
from simulator.scenarios import make_scenario, normal_txn
from stream_processor.types import AccountProfile, GraphFeatures

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def flow_ok(es: list[dict], since: float, amount: float, epoch: float) -> bool:
    if any(e["last_ts"] < since for e in es):
        return False
    for a, b in zip(es, es[1:]):
        if not (a["last_amount"] * 0.80 <= b["last_amount"] <= a["last_amount"] * 1.02
                and b["last_ts"] >= a["last_ts"]):
            return False
    last = es[-1]
    return last["last_amount"] * 1.02 >= amount and last["last_amount"] * 0.80 <= amount \
        and last["last_ts"] <= epoch


class InMemoryGraph:
    def __init__(self, pop: Population, high_risk: list[str]) -> None:
        self.pop, self.hr = pop, frozenset(high_risk)
        self.edges: dict[tuple[str, str], dict] = {}
        self.out: dict[str, set[str]] = defaultdict(set)
        self.inn: dict[str, set[str]] = defaultdict(set)
        self.addr = Counter(c.address_id for c in pop.customers)
        self.dev = Counter(c.device_id for c in pop.customers)

    def high_risk_countries(self) -> frozenset[str]:
        return self.hr

    def profile(self, account_id: str) -> AccountProfile:
        c = self.pop.customer_of(account_id)
        return AccountProfile(c.kind, self.addr[c.address_id] - 1, self.dev[c.device_id] - 1)

    def material_edges(self, since: float, min_amount: float) -> list[tuple[str, str, float]]:
        return [(a, b, e["last_amount"]) for (a, b), e in self.edges.items()
                if e["last_ts"] >= since and e["last_amount"] >= min_amount]

    def _forward(self, node: str, depth: int, used: set, path: list, since: float):
        if path:
            yield list(path), node
        if depth == 0:
            return
        for nxt in self.out[node]:
            if (node, nxt) in used:
                continue
            e = self.edges[(node, nxt)]
            if e["last_ts"] < since:
                continue
            used.add((node, nxt)); path.append(e)
            yield from self._forward(nxt, depth - 1, used, path, since)
            path.pop(); used.discard((node, nxt))

    def _backward(self, node: str, depth: int, used: set, path: list, since: float):
        if path:
            yield list(reversed(path))
        if depth == 0:
            return
        for prv in self.inn[node]:
            if (prv, node) in used:
                continue
            e = self.edges[(prv, node)]
            if e["last_ts"] < since:
                continue
            used.add((prv, node)); path.append(e)
            yield from self._backward(prv, depth - 1, used, path, since)
            path.pop(); used.discard((prv, node))

    def analyze_and_store(self, txn: Transaction, since: float, run_flow: bool) -> GraphFeatures:
        epoch, src, dst = txn.ts.timestamp(), txn.src_account, txn.dst_account
        cycle_len, up = None, 0
        if run_flow:
            for es, end in self._forward(dst, 3, set(), [], since):
                if end == src and len(es) >= 2 and flow_ok(es, since, txn.amount, epoch):
                    cycle_len = max(cycle_len or 0, len(es) + 1)
            for es in self._backward(src, 3, set(), [], since):
                if len(es) >= 2 and flow_ok(es, since, txn.amount, epoch):
                    up = max(up, len(es))
        self.edges[(src, dst)] = {"last_ts": epoch, "last_amount": txn.amount}
        self.out[src].add(dst); self.inn[dst].add(src)
        return GraphFeatures(cycle_len, up)


def simulate(seed: int, duration_s: float = 600, tps: float = 10, scen_per_min: float = 6,
             n_customers: int = 2000, hr=("IR", "KP", "MM")):
    """Virtual-time replica of the simulator's emission loop. Returns (pop, events, truth)."""
    rng = random.Random(seed)
    pop = build_population(rng, n_customers, list(hr))
    normal_accts = pop.accounts_in("person", "org")
    heap, seq = [], 0

    def push(t, p, sc):
        nonlocal seq
        seq += 1
        heapq.heappush(heap, (t, seq, p, sc))

    t = 0.0
    while t < duration_s:
        t += rng.expovariate(tps)
        push(t, normal_txn(rng, normal_accts), None)
    t = 0.0
    while True:
        t += rng.expovariate(scen_per_min / 60.0)
        if t >= duration_s:
            break
        sc = make_scenario(rng, pop)
        for p in sc.txns:
            push(t + p.delay_s, p, sc)

    events, truth, n = [], {}, 0
    while heap:
        t, _, p, sc = heapq.heappop(heap)
        n += 1
        src, dst = pop.account_by_id[p.src_account], pop.account_by_id[p.dst_account]
        cust = pop.customer_by_id[src.customer_id]
        txn = Transaction(txn_id=f"TX-{n:08d}", ts=T0 + timedelta(seconds=t), src_account=src.account_id,
                          dst_account=dst.account_id, amount=p.amount, channel=p.channel,
                          src_country=src.country, dst_country=dst.country, device_id=cust.device_id,
                          ip="10.0.0.1", narrative=p.narrative)
        events.append(txn)
        truth[txn.txn_id] = (sc.typology, sc.scenario_id) if sc else (None, None)
    return pop, events, truth


def run_with_analytics(pop, events, redis_client, use_net=True, interval_s=60.0, window_s=1800.0,
                       min_amount=5000.0, ontology=None, cfg=None, ttl_s=240, min_cluster=3):
    """Replay events through the hot path; every `interval_s` of virtual time run the cold path
    (NetworkxEngine + netscore + the real Redis writer) exactly as graph_analytics.main does."""
    from graph_analytics.engines import NetworkxEngine
    from graph_analytics.netscore import NetConfig, build_features, cluster_alerts
    from graph_analytics.store import write_redis
    from stream_processor.features import FeatureStore
    from stream_processor.ontology import DEFAULT_ONTOLOGY
    from stream_processor.pipeline import Pipeline
    from stream_processor.rules import RuleConfig

    graph = InMemoryGraph(pop, ["IR", "KP", "MM"])
    cfg = cfg or RuleConfig(use_network=use_net)
    pipe = Pipeline(FeatureStore(redis_client, read_net=use_net), graph, ontology or DEFAULT_ONTOLOGY, cfg)
    engine, decisions, runs, alerts, seen = NetworkxEngine(), {}, [], [], set()
    next_run = T0.timestamp() + interval_s
    for t in events:
        ts = t.ts.timestamp()
        while use_net and ts >= next_run:
            edges = graph.material_edges(next_run - window_s, min_amount)
            feats, clusters = build_features(edges, engine.analyze(edges), NetConfig())
            for a in cluster_alerts(feats, clusters, next_run, min_cluster):
                if a["signature"] not in seen:
                    seen.add(a["signature"]); alerts.append(a)
            runs.append((len(edges), write_redis(redis_client, feats, next_run, ttl_s)))
            next_run += interval_s
        decisions[t.txn_id] = pipe.process(t, now=t.ts)
    return decisions, runs, alerts
