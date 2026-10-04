"""Redis sliding-window features (event-time). One pipelined round trip per transaction.
All writes are idempotent (members include txn_id), so Kafka replays do not double count."""
from __future__ import annotations

import json
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
    src_net: dict | None = None        # cold-path network flags for the sender (see graph_analytics)
    dst_net: dict | None = None


class FeatureStore:
    def __init__(self, client, cfg: FeatureConfig | None = None, read_net: bool = True,
                 net_max_age_s: float = 240.0) -> None:
        self.r = client          # redis.Redis(decode_responses=True) or fakeredis
        self.cfg = cfg or FeatureConfig()
        self.read_net, self.net_max_age_s = read_net, net_max_age_s

    def _parse_net(self, raw: str | None, ts: float) -> dict | None:
        """Cold-path flags are ignored when older than net_max_age_s in event time (stale analytics)."""
        if not raw:
            return None
        try:
            d = json.loads(raw)
        except ValueError:
            return None
        return d if ts - float(d.get("ts", 0)) <= self.net_max_age_s else None

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
        if self.read_net:
            p.get(f"net:{src}")
            p.get(f"net:{dst}")
        res = p.execute()
        src_net = dst_net = None
        if self.read_net:
            src_net, dst_net = self._parse_net(res[-2], ts), self._parse_net(res[-1], ts)
            res = res[:-2]
        out_count, in_members, src_in_members, near_count = res[-4:]

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
            src_net=src_net, dst_net=dst_net,
        )
