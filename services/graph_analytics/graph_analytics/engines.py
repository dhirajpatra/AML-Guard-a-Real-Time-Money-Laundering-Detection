"""Graph algorithm engines. Both return the same AlgoResult.

GdsEngine      - runs WCC / SCC / PageRank (+ Louvain for oversized clusters) inside Neo4j Graph Data
                 Science: scales with the graph.
NetworkxEngine - pure-Python fallback / reference (small graphs, tests, GDS unavailable).

Clusters = weakly connected components of the material-flow graph. Louvain is only used to split
components larger than `max_cluster`: on small sparse structures (a 4-account ring) modularity
optimisation fragments the very pattern we want to keep together.
"""
from __future__ import annotations

import logging
import uuid
from collections import Counter

from .netscore import AlgoResult

log = logging.getLogger("analytics.engine")

Edge = tuple[str, str, float]


def pagerank(nodes: list[str], edges: list[Edge], damping: float = 0.85, iters: int = 100,
             tol: float = 1e-9) -> dict[str, float]:
    """Weighted PageRank by power iteration (dangling mass redistributed uniformly)."""
    n = len(nodes)
    if n == 0:
        return {}
    out_w: dict[str, float] = {a: 0.0 for a in nodes}
    for s, _, w in edges:
        out_w[s] += w
    rank = {a: 1.0 / n for a in nodes}
    for _ in range(iters):
        dangling = sum(rank[a] for a in nodes if out_w[a] == 0)
        nxt = {a: (1 - damping) / n + damping * dangling / n for a in nodes}
        for s, t, w in edges:
            nxt[t] += damping * rank[s] * w / out_w[s]
        delta = sum(abs(nxt[a] - rank[a]) for a in nodes)
        rank = nxt
        if delta < tol:
            break
    return rank


class NetworkxEngine:
    name = "networkx"

    def __init__(self, max_cluster: int = 40) -> None:
        self.max_cluster = max_cluster

    def analyze(self, edges: list[Edge], since: float = 0.0, min_amount: float = 0.0) -> AlgoResult:
        import networkx as nx

        d = nx.DiGraph()
        for s, t, a in edges:
            if d.has_edge(s, t):
                d[s][t]["w"] = max(d[s][t]["w"], a)
            else:
                d.add_edge(s, t, w=a)
        if d.number_of_nodes() == 0:
            return AlgoResult()
        und = d.to_undirected()
        communities: dict[str, object] = {}
        for i, comp in enumerate(nx.connected_components(und)):
            if len(comp) <= self.max_cluster:
                communities.update({n: str(i) for n in comp})
            else:
                parts = nx.community.louvain_communities(und.subgraph(comp), weight="w", seed=42)
                for j, part in enumerate(parts):
                    communities.update({n: f"{i}.{j}" for n in part})
        scc = {n: len(c) for c in nx.strongly_connected_components(d) for n in c}
        pr = pagerank(list(d.nodes), [(s, t, w["w"]) for s, t, w in d.edges(data=True)])
        return AlgoResult(communities, scc, pr)


class GdsEngine:
    name = "gds"

    PROJECT_D = """
    MATCH (s:Account)-[f:TRANSFERRED_TO]->(t:Account)
    WHERE f.last_ts >= $since AND f.last_amount >= $min_amount
    RETURN gds.graph.project($name, s, t, {relationshipProperties: {w: f.last_amount}})
    """
    PROJECT_U = """
    MATCH (s:Account)-[f:TRANSFERRED_TO]->(t:Account)
    WHERE f.last_ts >= $since AND f.last_amount >= $min_amount
    RETURN gds.graph.project($name, s, t, {relationshipProperties: {w: f.last_amount}},
                             {undirectedRelationshipTypes: ['*']})
    """
    WCC = """CALL gds.wcc.stream($name) YIELD nodeId, componentId
             RETURN gds.util.asNode(nodeId).account_id AS account, componentId AS cid"""
    LOUVAIN = """CALL gds.louvain.stream($name, {relationshipWeightProperty: 'w'}) YIELD nodeId, communityId
                 RETURN gds.util.asNode(nodeId).account_id AS account, communityId AS cid"""
    SCC = """CALL gds.scc.stream($name) YIELD nodeId, componentId
             RETURN gds.util.asNode(nodeId).account_id AS account, componentId AS cid"""
    PAGERANK = """CALL gds.pageRank.stream($name, {relationshipWeightProperty: 'w'}) YIELD nodeId, score
                  RETURN gds.util.asNode(nodeId).account_id AS account, score"""

    def __init__(self, driver, max_cluster: int = 40) -> None:
        self.driver, self.max_cluster = driver, max_cluster

    def _drop(self, names: list[str]) -> None:
        with self.driver.session() as s:
            for g in names:
                try:
                    s.run("CALL gds.graph.drop($name, false)", name=g).consume()
                except Exception:  # noqa: BLE001
                    pass

    def analyze(self, edges: list[Edge], since: float, min_amount: float) -> AlgoResult:
        if not edges:       # GDS cannot project an empty graph
            return AlgoResult()
        tag = uuid.uuid4().hex[:8]
        nd, nu = f"net_d_{tag}", f"net_u_{tag}"
        made = [nd]
        try:
            with self.driver.session() as s:
                s.run(self.PROJECT_D, name=nd, since=since, min_amount=min_amount).consume()
                wcc = {r["account"]: r["cid"] for r in s.run(self.WCC, name=nd)}
                comp = {r["account"]: r["cid"] for r in s.run(self.SCC, name=nd)}
                pr = {r["account"]: float(r["score"]) for r in s.run(self.PAGERANK, name=nd)}
                communities: dict[str, object] = {a: f"w{c}" for a, c in wcc.items()}
                big = {c for c, n in Counter(wcc.values()).items() if n > self.max_cluster}
                if big:        # split oversized components with Louvain on an undirected projection
                    s.run(self.PROJECT_U, name=nu, since=since, min_amount=min_amount).consume()
                    made.append(nu)
                    for r in s.run(self.LOUVAIN, name=nu):
                        if wcc.get(r["account"]) in big:
                            communities[r["account"]] = f"w{wcc[r['account']]}.{r['cid']}"
        finally:
            self._drop(made)
        sizes = Counter(comp.values())
        return AlgoResult(communities, {a: sizes[c] for a, c in comp.items()}, pr)
