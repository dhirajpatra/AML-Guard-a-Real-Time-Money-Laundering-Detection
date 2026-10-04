"""Pure detection logic: indicators -> typology scores (via the ontology) -> verdict."""
from __future__ import annotations

from dataclasses import dataclass

from aml_common.models import Transaction

from .features import RedisFeatures
from .ontology import Typology
from .types import AccountProfile, GraphFeatures

# How strongly each indicator, when fired, supports its typologies (0..1).
STRENGTH: dict[str, float] = {
    "JUST_BELOW_THRESHOLD": 0.80,
    "HIGH_VELOCITY": 0.50,
    "MANY_TO_ONE": 0.90,
    "SHARED_DEVICE": 0.50,
    "SHARED_ADDRESS": 0.35,
    "HIGH_RISK_JURISDICTION": 0.70,
    "CYCLE": 1.00,
    "LAYERED_HOPS": 0.80,
    "RAPID_PASS_THROUGH": 0.90,
    # cold-path (graph analytics) indicators
    "CIRCULAR_FLOW": 0.70,
    "SUSPICIOUS_CLUSTER": 0.50,
    "HUB_CENTRALITY": 0.40,
}


@dataclass(frozen=True)
class RuleConfig:
    structuring_min_count: int = 3
    velocity_min_out: int = 5
    fan_in_min_senders: int = 6
    fan_in_min_sum: float = 8000.0
    passthrough_ratio_lo: float = 0.90
    passthrough_min_amount: float = 5000.0
    layered_min_up: int = 2
    flow_min_amount: float = 5000.0
    use_network: bool = True
    net_min_amount: float = 5000.0     # network context only matters for material flows
    review_threshold: float = 0.50
    block_threshold: float = 0.80


def noisy_or(ps: list[float]) -> float:
    q = 1.0
    for p in ps:
        q *= 1.0 - p
    return 1.0 - q


def derive_indicators(txn: Transaction, rf: RedisFeatures, gf: GraphFeatures,
                      src: AccountProfile, dst: AccountProfile, high_risk: frozenset[str],
                      cfg: RuleConfig) -> tuple[dict[str, float], dict]:
    fired: dict[str, float] = {}
    ev: dict = {}

    if rf.is_near_threshold and rf.near_count >= cfg.structuring_min_count:
        fired["JUST_BELOW_THRESHOLD"] = STRENGTH["JUST_BELOW_THRESHOLD"]
        ev["near_threshold_count_1h"] = rf.near_count
    if rf.out_count >= cfg.velocity_min_out:
        fired["HIGH_VELOCITY"] = STRENGTH["HIGH_VELOCITY"]
        ev["sent_last_120s"] = rf.out_count
    if rf.in_senders >= cfg.fan_in_min_senders and rf.in_sum >= cfg.fan_in_min_sum:
        fired["MANY_TO_ONE"] = STRENGTH["MANY_TO_ONE"]
        ev["distinct_senders_120s"] = rf.in_senders
        ev["received_sum_120s"] = rf.in_sum
    if src.dev_peers >= 1:
        fired["SHARED_DEVICE"] = STRENGTH["SHARED_DEVICE"]
        ev["device_peers"] = src.dev_peers
    peers = max(src.addr_peers, dst.addr_peers)
    if peers >= 1:
        fired["SHARED_ADDRESS"] = STRENGTH["SHARED_ADDRESS"]
        ev["address_peers"] = peers
    if txn.src_country in high_risk or txn.dst_country in high_risk:
        fired["HIGH_RISK_JURISDICTION"] = STRENGTH["HIGH_RISK_JURISDICTION"]
        ev["high_risk_countries"] = sorted({txn.src_country, txn.dst_country} & high_risk)
    if gf.cycle_len is not None:
        fired["CYCLE"] = STRENGTH["CYCLE"]
        ev["cycle_length"] = gf.cycle_len
    if gf.up_chain_len >= cfg.layered_min_up:
        fired["LAYERED_HOPS"] = STRENGTH["LAYERED_HOPS"]
        ev["chain_hops"] = gf.up_chain_len + 1
    if txn.amount >= cfg.passthrough_min_amount and any(
            a > 0 and cfg.passthrough_ratio_lo <= txn.amount / a <= 1.0 for a in rf.recent_inbound_to_src):
        fired["RAPID_PASS_THROUGH"] = STRENGTH["RAPID_PASS_THROUGH"]
        ev["forwarded_from_recent_inbound"] = True
    if cfg.use_network and txn.amount >= cfg.net_min_amount:
        nets = [n for n in (rf.src_net, rf.dst_net) if n]
        if any(n.get("c") for n in nets):
            fired["CIRCULAR_FLOW"] = STRENGTH["CIRCULAR_FLOW"]
            ev["circular_flow_cluster"] = sorted({n["cl"] for n in nets if n.get("c")})
        if any(n.get("t") for n in nets):
            fired["SUSPICIOUS_CLUSTER"] = STRENGTH["SUSPICIOUS_CLUSTER"]
            ev["suspicious_cluster"] = sorted({n["cl"] for n in nets if n.get("t")})
        if any(n.get("h") for n in nets):
            fired["HUB_CENTRALITY"] = STRENGTH["HUB_CENTRALITY"]
            ev["hub_cluster"] = sorted({n["cl"] for n in nets if n.get("h")})
    return fired, ev


def score_typologies(fired: dict[str, float],
                     ontology: dict[str, Typology]) -> tuple[float, str | None, dict[str, float]]:
    """typology score = base_weight x noisy-OR(strengths of its fired indicators)."""
    best, best_id, per = 0.0, None, {}
    for t in ontology.values():
        ss = [fired[i] for i in t.indicators if i in fired]
        if not ss:
            continue
        sc = t.base_weight * noisy_or(ss)
        per[t.id] = round(sc, 4)
        if sc > best:
            best, best_id = sc, t.id
    return best, best_id, per


def verdict_for(score: float, cfg: RuleConfig) -> str:
    if score >= cfg.block_threshold:
        return "BLOCK"
    if score >= cfg.review_threshold:
        return "REVIEW"
    return "PASS"
