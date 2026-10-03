"""End-to-end through Pipeline with fakeredis + the in-memory graph twin (virtual clock)."""
import fakeredis
import pytest

from stream_processor.features import FeatureStore
from stream_processor.ontology import DEFAULT_ONTOLOGY
from stream_processor.pipeline import Pipeline
from stream_processor.rules import RuleConfig

from .twin import InMemoryGraph, simulate


@pytest.fixture(scope="module")
def result():
    pop, events, truth = simulate(seed=11, duration_s=900)
    pipe = Pipeline(FeatureStore(fakeredis.FakeRedis(decode_responses=True)),
                    InMemoryGraph(pop, ["IR", "KP", "MM"]), DEFAULT_ONTOLOGY, RuleConfig())
    decisions = {t.txn_id: pipe.process(t, now=t.ts) for t in events}
    return decisions, truth


def test_every_scenario_is_detected(result):
    decisions, truth = result
    scen = {}
    for tid, (typ, sid) in truth.items():
        if sid:
            s = scen.setdefault(sid, {"typ": typ, "hit": False})
            s["hit"] |= decisions[tid].verdict != "PASS"
    assert len(scen) >= 60
    by_typ = {}
    for s in scen.values():
        b = by_typ.setdefault(s["typ"], [0, 0]); b[0] += 1; b[1] += s["hit"]
    print("\nscenario detection:", by_typ)
    assert set(by_typ) == {"STRUCTURING", "FAN_IN", "ROUND_TRIP", "RAPID_PASS_THROUGH", "SHELL_LAYERING"}
    for typ, (n, hit) in by_typ.items():
        assert hit / n >= 0.95, (typ, n, hit)


def test_normal_traffic_rarely_flagged(result):
    decisions, truth = result
    normal = [d for tid, d in decisions.items() if truth[tid][1] is None]
    fp = [d for d in normal if d.verdict != "PASS"]
    print(f"\nnormal txns: {len(normal)}, flagged: {len(fp)} ({len(fp) / len(normal):.4%})")
    assert len(normal) > 8000
    assert len(fp) / len(normal) < 0.002


def test_peak_attribution_matches_ground_truth(result):
    """Intermediate hops of a ring look like layering/pass-through until it closes, so attribution is
    judged on each scenario's highest-risk transaction (what an analyst would open first)."""
    decisions, truth = result
    peak = {}
    for tid, (typ, sid) in truth.items():
        if sid and (sid not in peak or decisions[tid].risk_score >= peak[sid][0]):
            peak[sid] = (decisions[tid].risk_score, decisions[tid].typology, typ)
    right = sum(1 for _, got, want in peak.values() if got == want)
    print(f"\npeak typology correct: {right}/{len(peak)} ({right / len(peak):.1%})")
    assert right / len(peak) >= 0.80


def test_block_is_reserved_for_strong_evidence(result):
    decisions, _ = result
    blocks = [d for d in decisions.values() if d.verdict == "BLOCK"]
    assert blocks and all(d.typology in {"ROUND_TRIP", "SHELL_LAYERING"} for d in blocks)
