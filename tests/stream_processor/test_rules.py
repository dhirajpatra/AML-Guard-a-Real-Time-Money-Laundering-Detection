import re
from datetime import datetime, timezone
from pathlib import Path

from aml_common.models import Transaction
from stream_processor.features import RedisFeatures
from stream_processor.ontology import DEFAULT_ONTOLOGY
from stream_processor.rules import (RuleConfig, derive_indicators, noisy_or, score_typologies, verdict_for)
from stream_processor.stats import percentile, summarize
from stream_processor.types import AccountProfile, GraphFeatures

CFG = RuleConfig()
P = AccountProfile("organization", 0, 0)


def txn(amount=1000.0, sc="IN", dc="IN"):
    return Transaction(txn_id="T1", ts=datetime(2026, 1, 1, tzinfo=timezone.utc), src_account="A", dst_account="B",
                       amount=amount, channel="wire", src_country=sc, dst_country=dc)


def run(t, rf=None, gf=None, src=P, dst=P, hr=frozenset({"IR"})):
    fired, ev = derive_indicators(t, rf or RedisFeatures(), gf or GraphFeatures(), src, dst, hr, CFG)
    score, typ, _ = score_typologies(fired, DEFAULT_ONTOLOGY)
    return fired, score, typ, verdict_for(score, CFG)


def test_ontology_fallback_matches_graph_schema():
    text = Path("graph/01_schema.cypher").read_text()
    found = re.findall(r"id:'([A-Z_]+)'.*?weight:([0-9.]+),\s*indicators:\[([^\]]*)\]", text, re.S)
    assert len(found) == len(DEFAULT_ONTOLOGY) == 5
    for tid, w, inds in found:
        t = DEFAULT_ONTOLOGY[tid]
        assert t.base_weight == float(w)
        assert set(t.indicators) == set(re.findall(r"'([A-Z_]+)'", inds))


def test_noisy_or():
    assert noisy_or([]) == 0.0
    assert abs(noisy_or([0.5, 0.5]) - 0.75) < 1e-9


def test_ordinary_payment_passes():
    assert run(txn())[3] == "PASS"


def test_weak_signals_alone_do_not_alert():
    rf = RedisFeatures(out_count=6)
    assert run(txn(), rf)[3] == "PASS"
    assert run(txn(), src=AccountProfile("person", 0, 2))[3] == "PASS"
    assert run(txn(), dst=AccountProfile("organization", 2, 0))[3] == "PASS"


def test_structuring_needs_repetition():
    one = RedisFeatures(is_near_threshold=True, near_count=1)
    three = RedisFeatures(is_near_threshold=True, near_count=3)
    assert run(txn(9500), one)[3] == "PASS"
    _, score, typ, v = run(txn(9500), three)
    assert typ == "STRUCTURING" and v == "REVIEW" and 0.5 <= score < 0.8


def test_fan_in():
    rf = RedisFeatures(in_senders=7, in_sum=15000.0)
    _, _, typ, v = run(txn(2000), rf)
    assert typ == "FAN_IN" and v == "REVIEW"
    assert run(txn(2000), RedisFeatures(in_senders=7, in_sum=3000.0))[3] == "PASS"


def test_cycle_blocks():
    _, score, typ, v = run(txn(40000), gf=GraphFeatures(cycle_len=3, up_chain_len=2))
    assert typ == "ROUND_TRIP" and v == "BLOCK" and score >= 0.8


def test_rapid_pass_through():
    rf = RedisFeatures(recent_inbound_to_src=[30000.0])
    assert run(txn(29400), rf)[2] == "RAPID_PASS_THROUGH"
    assert run(txn(29400), rf)[3] == "REVIEW"
    assert run(txn(12000), rf)[3] == "PASS"            # forwards only 40% -> not pass-through
    assert run(txn(900), RedisFeatures(recent_inbound_to_src=[950.0]))[3] == "PASS"   # immaterial amount


def test_shell_layering_into_high_risk_blocks():
    gf = GraphFeatures(up_chain_len=2)
    assert run(txn(50000), gf=gf)[3] == "REVIEW"
    _, _, typ, v = run(txn(50000, dc="IR"), gf=gf)
    assert typ == "SHELL_LAYERING" and v == "BLOCK"


def test_percentiles():
    assert percentile([], 50) == 0.0
    assert percentile([1, 2, 3, 4, 5], 50) == 3
    s = summarize([float(i) for i in range(1, 101)])
    assert s["n"] == 100 and 49 <= s["p50"] <= 52 and s["max"] == 100.0


def net(**k):
    return {"ts": 0, "c": False, "t": False, "h": False, "cl": "CL-X", "cs": 5, "scc": 1, **k}


def test_network_indicators_corroborate_but_do_not_stand_alone_when_weak():
    big = txn(40000)
    assert run(big, RedisFeatures(src_net=net(t=True)))[3] == "PASS"                    # tight cluster alone: 0.45
    f, score, typ, v = run(big, RedisFeatures(dst_net=net(c=True)))
    assert "CIRCULAR_FLOW" in f and typ == "ROUND_TRIP" and v == "REVIEW"               # early ring warning
    assert run(big, RedisFeatures(src_net=net(h=True)))[3] == "PASS"                    # hub alone: weak
    # corroborated: cluster + shared address -> review for shell layering
    assert run(big, RedisFeatures(src_net=net(t=True)), dst=AccountProfile("organization", 2, 0))[3] == "REVIEW"


def test_network_flags_ignored_for_immaterial_amounts_or_when_disabled():
    rf = RedisFeatures(src_net=net(c=True, t=True))
    assert run(txn(800), rf)[3] == "PASS"
    cfg = RuleConfig(use_network=False)
    fired, _ = derive_indicators(txn(40000), rf, GraphFeatures(), P, P, frozenset(), cfg)
    assert "CIRCULAR_FLOW" not in fired
