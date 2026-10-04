"""Pure scoring of network structure -> per-account flags (engine-independent, unit-tested).

Inputs are algorithm outputs (clusters = connected components, SCC sizes, PageRank) over the *material recent flow graph*.
Flags (stored for the hot path):
  circular (c): account sits in a strongly connected component of >= scc_min accounts (money loops back)
  tight    (t): account is in a community of >= tight_size accounts whose flow is >= tight_ratio internal
  hub      (h): account is a PageRank hub (>= hub_factor x cluster mean) with >= hub_min_indegree senders
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass(frozen=True)
class NetConfig:
    tight_size: int = 4
    tight_ratio: float = 0.8
    scc_min: int = 3
    hub_factor: float = 2.0
    hub_min_indegree: int = 3
    hub_min_cluster: int = 4


@dataclass
class AlgoResult:
    communities: dict[str, object] = field(default_factory=dict)   # account -> community id
    scc_size: dict[str, int] = field(default_factory=dict)         # account -> size of its SCC
    pagerank: dict[str, float] = field(default_factory=dict)


@dataclass
class NetFeatures:
    account: str
    cluster_id: str
    cluster_size: int
    scc_size: int
    pagerank: float
    circular: bool
    tight: bool
    hub: bool

    @property
    def flagged(self) -> bool:
        return self.circular or self.tight or self.hub


@dataclass
class ClusterInfo:
    cluster_id: str
    members: list[str]
    internal_ratio: float
    total_flow: float


def build_features(edges: list[tuple[str, str, float]], res: AlgoResult,
                   cfg: NetConfig | None = None) -> tuple[dict[str, NetFeatures], dict[str, ClusterInfo]]:
    cfg = cfg or NetConfig()
    members: dict[object, list[str]] = defaultdict(list)
    for acct, cid in res.communities.items():
        members[cid].append(acct)
    cluster_name = {cid: f"CL-{min(m)}" for cid, m in members.items()}

    internal: dict[object, float] = defaultdict(float)
    touching: dict[object, float] = defaultdict(float)
    senders: dict[str, set[str]] = defaultdict(set)
    for s, t, amt in edges:
        senders[t].add(s)
        cs, ct = res.communities.get(s), res.communities.get(t)
        if cs is None or ct is None:
            continue
        touching[cs] += amt
        if cs == ct:
            internal[cs] += amt
        else:
            touching[ct] += amt

    clusters: dict[str, ClusterInfo] = {}
    ratio: dict[object, float] = {}
    mean_pr: dict[object, float] = {}
    for cid, m in members.items():
        ratio[cid] = internal[cid] / touching[cid] if touching[cid] else 0.0
        mean_pr[cid] = sum(res.pagerank.get(a, 0.0) for a in m) / len(m)
        clusters[cluster_name[cid]] = ClusterInfo(cluster_name[cid], sorted(m), round(ratio[cid], 4),
                                                  round(touching[cid], 2))

    feats: dict[str, NetFeatures] = {}
    for acct, cid in res.communities.items():
        size = len(members[cid])
        pr = res.pagerank.get(acct, 0.0)
        scc = res.scc_size.get(acct, 1)
        feats[acct] = NetFeatures(
            account=acct, cluster_id=cluster_name[cid], cluster_size=size, scc_size=scc, pagerank=pr,
            circular=scc >= cfg.scc_min,
            tight=size >= cfg.tight_size and ratio[cid] >= cfg.tight_ratio,
            hub=size >= cfg.hub_min_cluster and len(senders[acct]) >= cfg.hub_min_indegree
                and mean_pr[cid] > 0 and pr >= cfg.hub_factor * mean_pr[cid],
        )
    return feats, clusters


def cluster_alerts(feats: dict[str, NetFeatures], clusters: dict[str, ClusterInfo], run_ts: float,
                   min_size: int = 3) -> list[dict]:
    """Cluster-level (network) alerts: closed loops and tight clusters. Hubs alone do not alert.

    These catch what the real-time path structurally cannot: patterns whose hops are spread over
    longer than the hot path's flow window, or loops longer than its 3-hop search."""
    out: list[dict] = []
    for cid, c in clusters.items():
        if len(c.members) < min_size:
            continue
        fs = [feats[m] for m in c.members]
        circular = sorted(f.account for f in fs if f.circular)
        if not (circular or any(f.tight for f in fs)):
            continue
        pattern = "closed_loop" if circular else "tight_cluster"
        digest = hashlib.sha1("|".join(c.members).encode()).hexdigest()[:8]
        out.append({
            "kind": "network_cluster", "signature": f"{cid}:{digest}:{pattern}", "pattern": pattern,
            "cluster_id": cid, "members": c.members, "size": len(c.members),
            "internal_ratio": c.internal_ratio, "total_flow": c.total_flow,
            "circular_members": circular, "hubs": sorted(f.account for f in fs if f.hub), "run_ts": run_ts,
        })
    return out
