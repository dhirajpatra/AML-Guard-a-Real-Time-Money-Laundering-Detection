import fakeredis

from graph_analytics.engines import NetworkxEngine, pagerank
from graph_analytics.netscore import AlgoResult, NetConfig, build_features
from graph_analytics.store import NET_INDEX, write_redis

NX = NetworkxEngine()


def feats_for(edges):
    res = NX.analyze(edges)
    return build_features(edges, res, NetConfig())


def test_ring_is_circular_and_tight():
    edges = [("A", "B", 50000), ("B", "C", 49000), ("C", "D", 48000), ("D", "A", 47000)]
    feats, clusters = feats_for(edges)
    assert all(f.circular and f.tight for f in feats.values())
    assert {f.cluster_id for f in feats.values()} == {"CL-A"}
    assert clusters["CL-A"].internal_ratio == 1.0 and len(clusters["CL-A"].members) == 4


def test_chain_is_tight_but_not_circular():
    edges = [("A", "B", 90000), ("B", "C", 88000), ("C", "D", 86000), ("D", "E", 84000)]
    feats, _ = feats_for(edges)
    assert all(f.tight and not f.circular for f in feats.values())


def test_isolated_pairs_and_short_paths_are_not_flagged():
    edges = [("A", "B", 7000), ("C", "D", 9000), ("E", "F", 6000), ("F", "G", 5900)]
    feats, _ = feats_for(edges)
    assert not any(f.flagged for f in feats.values())


def test_hub_detected_in_star_of_material_flows():
    edges = [(f"S{i}", "HUB", 8000) for i in range(6)] + [("HUB", "OUT", 40000)]
    feats, _ = feats_for(edges)
    assert feats["HUB"].hub
    assert not feats["S0"].hub


def test_low_internal_ratio_blocks_tight_flag():
    res = AlgoResult(communities={"A": 0, "B": 0, "C": 0, "D": 0, "X": 1, "Y": 2},
                     scc_size={}, pagerank={})
    edges = [("A", "B", 100), ("B", "C", 100), ("C", "D", 100), ("A", "X", 1000), ("Y", "D", 1000)]
    feats, clusters = build_features(edges, res, NetConfig())
    assert clusters["CL-A"].internal_ratio < 0.8 and not feats["A"].tight


def test_pagerank_sums_to_one_and_favours_sinks():
    pr = pagerank(["A", "B", "C"], [("A", "C", 1.0), ("B", "C", 1.0)])
    assert abs(sum(pr.values()) - 1.0) < 1e-6 and pr["C"] > pr["A"]


def test_engine_empty_graph():
    assert NX.analyze([]).communities == {}


def test_write_redis_publishes_flagged_and_removes_stale():
    r = fakeredis.FakeRedis(decode_responses=True)
    ring = [("A", "B", 50000), ("B", "C", 49000), ("C", "A", 48000)]
    feats, _ = feats_for(ring)
    assert write_redis(r, feats, 1000.0, 240) == 3
    assert r.exists("net:A") and r.smembers(NET_INDEX) == {"A", "B", "C"}
    # next run: only a different ring -> old accounts must be removed
    feats2, _ = feats_for([("X", "Y", 50000), ("Y", "Z", 49000), ("Z", "X", 48000)])
    write_redis(r, feats2, 1060.0, 240)
    assert not r.exists("net:A") and r.exists("net:X") and r.smembers(NET_INDEX) == {"X", "Y", "Z"}


def test_oversized_component_is_split_into_communities():
    blob1 = [(f"A{i}", f"A{(i + 1) % 5}", 9000) for i in range(5)] + [("A0", "A2", 9000), ("A1", "A3", 9000)]
    blob2 = [(f"B{i}", f"B{(i + 1) % 5}", 9000) for i in range(5)] + [("B0", "B2", 9000), ("B1", "B3", 9000)]
    edges = blob1 + blob2 + [("A4", "B0", 6000)]
    whole = NetworkxEngine(max_cluster=40).analyze(edges)
    assert len(set(whole.communities.values())) == 1          # one connected component stays one cluster
    split = NetworkxEngine(max_cluster=6).analyze(edges)
    assert len(set(split.communities.values())) >= 2
    assert split.communities["A0"] != split.communities["B0"]


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def poll(self, *_):
        return 0


def test_cluster_alerts_and_dedupe():
    from graph_analytics.netscore import cluster_alerts
    from graph_analytics.store import publish_cluster_alerts

    ring = [("A", "B", 50000), ("B", "C", 49000), ("C", "A", 48000)]
    noise = [("X", "Y", 7000), ("Y", "Z", 6500)]                    # size-3 path: not flagged
    feats, clusters = feats_for(ring + noise)
    alerts = cluster_alerts(feats, clusters, run_ts=1.0)
    assert len(alerts) == 1 and alerts[0]["pattern"] == "closed_loop"
    assert alerts[0]["members"] == ["A", "B", "C"] and alerts[0]["circular_members"] == ["A", "B", "C"]
    r, prod = fakeredis.FakeRedis(decode_responses=True), FakeProducer()
    assert publish_cluster_alerts(r, prod, "alerts.network", alerts) == 1
    assert publish_cluster_alerts(r, prod, "alerts.network", alerts) == 0     # same cluster: not re-sent
    assert len(prod.sent) == 1 and prod.sent[0][0] == "alerts.network"


def test_chain_gives_tight_cluster_alert_without_circular():
    from graph_analytics.netscore import cluster_alerts
    feats, clusters = feats_for([("A", "B", 90000), ("B", "C", 88000), ("C", "D", 86000), ("D", "E", 84000)])
    (a,) = cluster_alerts(feats, clusters, 1.0)
    assert a["pattern"] == "tight_cluster" and a["circular_members"] == []
