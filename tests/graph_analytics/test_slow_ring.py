"""A ring whose hops are 8 minutes apart is invisible to the hot path (5-minute flow window) but is
found by the cold path, which surfaces it as a cluster alert within one analytics interval."""
import random
from datetime import timedelta

import fakeredis

from aml_common.models import Transaction
from simulator.population import build_population
from tests.stream_processor.twin import T0, run_with_analytics


def test_slow_ring_found_by_cold_path_only():
    pop = build_population(random.Random(3), 400, ["IR", "KP", "MM"])
    ring = [a for a in pop.accounts_in("org")][:4]
    events, amount = [], 40000.0
    for i in range(4):
        src, dst = ring[i], ring[(i + 1) % 4]
        events.append(Transaction(
            txn_id=f"SLOW-{i}", ts=T0 + timedelta(seconds=60 + i * 480), src_account=src.account_id,
            dst_account=dst.account_id, amount=round(amount, 2), channel="wire", src_country=src.country,
            dst_country=dst.country, device_id="D", ip="10.0.0.1"))
        amount *= 0.97
    # pad the clock so the cold path runs after the ring closes (last hop at t=1500s)
    events.append(events[-1].model_copy(update={"txn_id": "PAD", "src_account": pop.accounts_in("person")[0].account_id,
                                                  "dst_account": pop.accounts_in("person")[1].account_id,
                                                  "amount": 50.0, "ts": T0 + timedelta(seconds=1700)}))
    members = sorted(a.account_id for a in ring)

    off, _, off_alerts = run_with_analytics(pop, events, fakeredis.FakeRedis(decode_responses=True), use_net=False)
    assert all(off[f"SLOW-{i}"].verdict == "PASS" for i in range(4)), "hot path should miss a slow ring"
    assert off_alerts == []

    on, _, alerts = run_with_analytics(pop, events, fakeredis.FakeRedis(decode_responses=True), use_net=True)
    loops = [a for a in alerts if a["pattern"] == "closed_loop" and a["members"] == members]
    assert loops, alerts
    assert loops[0]["run_ts"] - events[3].ts.timestamp() <= 60      # surfaced within one interval of closing
