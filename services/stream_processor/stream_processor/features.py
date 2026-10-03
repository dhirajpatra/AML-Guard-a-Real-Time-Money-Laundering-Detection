"""Redis sliding-window features (event-time). One pipelined round trip per transaction.
All writes are idempotent (members include txn_id), so Kafka replays do not double count."""
from __future__ import annotations

from dataclasses import dataclass, field

from aml_common.models import Transaction


@dataclass
class FeatureConfig:
    velocity_window_s: float = 120.0
    passthrough_window_s: float = 20.0
    structuring_window_s: float = 3600.0
    near_lo: float = 9000.0
    near_hi: float = 10000.0
    retention_s: float = 300.0


@dataclass
class RedisFeatures:
    out_count: int = 0                 # txns sent by src in velocity window (incl. this one)
    is_near_threshold: bool = False
    near_count: int = 0                # near-threshold txns by src in structuring window (incl. this one)
    in_count: int = 0                  # txns received by dst in velocity window (incl. this one)
    in_senders: int = 0                # distinct senders into dst in velocity window
    in_sum: float = 0.0
    recent_inbound_to_src: list[float] = field(default_factory=list)  # amounts src received just before


class FeatureStore:
    def __init__(self, client, cfg: FeatureConfig | None = None) -> None:
        self.r = client          # redis.Redis(decode_responses=True) or fakeredis
        self.cfg = cfg or FeatureConfig()

    def update_and_read(self, txn: Transaction) -> RedisFeatures:
        c = self.cfg
        ts = txn.ts.timestamp()
        src, dst = txn.src_account, txn.dst_account
        out_k, in_k, src_in_k, near_k = f"vel:out:{src}", f"vel:in:{dst}", f"vel:in:{src}", f"vel:near:{src}"
        near = c.near_lo <= txn.amount < c.near_hi

        p = self.r.pipeline(transaction=False)
        p.zadd(out_k, {txn.txn_id: ts})
        p.zadd(in_k, {f"{txn.txn_id}|{src}|{txn.amount}": ts})
        if near:
            p.zadd(near_k, {txn.txn_id: ts})
            p.expire(near_k, int(c.structuring_window_s * 2))
        p.zremrangebyscore(out_k, "-inf", ts - c.retention_s)
        p.zremrangebyscore(in_k, "-inf", ts - c.retention_s)
        p.expire(out_k, int(c.retention_s * 3))
        p.expire(in_k, int(c.retention_s * 3))
        p.zcount(out_k, ts - c.velocity_window_s, ts)
        p.zrangebyscore(in_k, ts - c.velocity_window_s, ts)
        p.zrangebyscore(src_in_k, ts - c.passthrough_window_s, ts)
        p.zcount(near_k, ts - c.structuring_window_s, ts)
        out_count, in_members, src_in_members, near_count = p.execute()[-4:]

        senders: set[str] = set()
        total = 0.0
        for m in in_members:
            _, s, a = m.split("|")
            senders.add(s)
            total += float(a)
        return RedisFeatures(
            out_count=int(out_count), is_near_threshold=near, near_count=int(near_count),
            in_count=len(in_members), in_senders=len(senders), in_sum=round(total, 2),
            recent_inbound_to_src=[float(m.split("|")[2]) for m in src_in_members],
        )
