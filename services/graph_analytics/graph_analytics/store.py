"""Writers for the cold path: Redis (hot-path flags), Neo4j (properties + :Cluster nodes), edge pruning."""
from __future__ import annotations

import json
import logging

from .netscore import ClusterInfo, NetFeatures

log = logging.getLogger("analytics.store")

NET_INDEX = "net:index"

# ---- Redis ----

def write_redis(client, feats: dict[str, NetFeatures], run_ts: float, ttl_s: int) -> int:
    """Publish flags for flagged accounts only; remove accounts that are no longer flagged."""
    flagged = {a: f for a, f in feats.items() if f.flagged}
    previous = set(client.smembers(NET_INDEX))
    p = client.pipeline(transaction=False)
    for a, f in flagged.items():
        p.set(f"net:{a}", json.dumps({"ts": run_ts, "c": f.circular, "t": f.tight, "h": f.hub,
                                      "cl": f.cluster_id, "cs": f.cluster_size, "scc": f.scc_size}),
              ex=ttl_s)
    stale = previous - set(flagged)
    if stale:
        p.delete(*[f"net:{a}" for a in stale])
    p.delete(NET_INDEX)
    if flagged:
        p.sadd(NET_INDEX, *flagged)
        p.expire(NET_INDEX, ttl_s * 2)
    p.execute()
    return len(flagged)


ALERTED_SET = "net:alerted"


def publish_cluster_alerts(client, producer, topic: str, alerts: list[dict], ttl_s: int = 21600) -> int:
    """Publish each distinct cluster alert once (signature dedupe in Redis)."""
    sent = 0
    for a in alerts:
        if client.sadd(ALERTED_SET, a["signature"]) == 1:
            producer.produce(topic, key=a["cluster_id"].encode(), value=json.dumps(a).encode())
            sent += 1
    if alerts:
        client.expire(ALERTED_SET, ttl_s)
        producer.poll(0)
    return sent


# ---- Neo4j ----

SET_ACCOUNTS_Q = """
UNWIND $rows AS r
MATCH (a:Account {account_id: r.account})
SET a.net_run = $run, a.net_cluster = r.cluster, a.net_cluster_size = r.size, a.net_scc_size = r.scc,
    a.net_pagerank = r.pr, a.net_circular = r.circular, a.net_tight = r.tight, a.net_hub = r.hub
"""
CLUSTERS_Q = """
UNWIND $clusters AS c
MERGE (k:Cluster {cluster_id: c.id})
  SET k.size = c.size, k.internal_ratio = c.ratio, k.total_flow = c.flow, k.run = $run
WITH k, c
UNWIND c.members AS m
MATCH (a:Account {account_id: m})
MERGE (a)-[:MEMBER_OF]->(k)
"""
CLEANUP_Q = """
MATCH (a:Account) WHERE a.net_run IS NOT NULL AND a.net_run <> $run
REMOVE a.net_run, a.net_cluster, a.net_cluster_size, a.net_scc_size, a.net_pagerank,
       a.net_circular, a.net_tight, a.net_hub
"""


def write_neo4j(driver, run_id: str, feats: dict[str, NetFeatures], clusters: dict[str, ClusterInfo],
                cluster_min_size: int = 3) -> None:
    rows = [dict(account=f.account, cluster=f.cluster_id, size=f.cluster_size, scc=f.scc_size,
                 pr=f.pagerank, circular=f.circular, tight=f.tight, hub=f.hub) for f in feats.values()]
    cl_rows = [dict(id=c.cluster_id, size=len(c.members), ratio=c.internal_ratio, flow=c.total_flow,
                    members=c.members) for c in clusters.values() if len(c.members) >= cluster_min_size]

    def work(tx) -> None:
        tx.run("MATCH (k:Cluster) DETACH DELETE k").consume()      # one tx: readers never see a gap
        if rows:
            tx.run(SET_ACCOUNTS_Q, rows=rows, run=run_id).consume()
        if cl_rows:
            tx.run(CLUSTERS_Q, clusters=cl_rows, run=run_id).consume()
        tx.run(CLEANUP_Q, run=run_id).consume()

    with driver.session() as s:
        s.execute_write(work)


GRAPH_TIME_Q = "MATCH ()-[f:TRANSFERRED_TO]->() RETURN max(f.last_ts) AS m"
EDGES_Q = """
MATCH (s:Account)-[f:TRANSFERRED_TO]->(t:Account)
WHERE f.last_ts >= $since AND f.last_amount >= $min_amount
RETURN s.account_id AS src, t.account_id AS dst, f.last_amount AS amount
"""
PRUNE_Q = """
MATCH ()-[f:TRANSFERRED_TO]->() WHERE f.last_ts < $cutoff
WITH f LIMIT $batch DELETE f RETURN count(*) AS n
"""


def graph_time(driver) -> float | None:
    """'Now' in graph/event time = newest flow edge. Keeps windows correct during backlog replay."""
    with driver.session() as s:
        rec = s.execute_read(lambda tx: tx.run(GRAPH_TIME_Q).single())
    return float(rec["m"]) if rec and rec["m"] is not None else None


def fetch_edges(driver, since: float, min_amount: float) -> list[tuple[str, str, float]]:
    with driver.session() as s:
        rows = s.execute_read(lambda tx: tx.run(EDGES_Q, since=since, min_amount=min_amount).data())
    return [(r["src"], r["dst"], float(r["amount"])) for r in rows]


def prune_edges(driver, cutoff: float, batch: int = 5000) -> int:
    total = 0
    while True:
        with driver.session() as s:
            n = s.execute_write(lambda tx: tx.run(PRUNE_Q, cutoff=cutoff, batch=batch).single()["n"])
        total += n
        if n < batch:
            return total
