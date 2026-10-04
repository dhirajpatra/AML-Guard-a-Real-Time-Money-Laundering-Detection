from datetime import datetime, timedelta, timezone

import fakeredis

from aml_common.models import Transaction
from stream_processor.features import FeatureStore

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def mk(i, src, dst, amount, dt):
    return Transaction(txn_id=f"T{i}", ts=T0 + timedelta(seconds=dt), src_account=src, dst_account=dst,
                       amount=amount, channel="wire", src_country="IN", dst_country="IN")


def store():
    return FeatureStore(fakeredis.FakeRedis(decode_responses=True))


def test_fan_in_counts_distinct_senders_in_window():
    fs = store()
    for i in range(8):
        rf = fs.update_and_read(mk(i, f"S{i}", "COLLECTOR", 2000.0, i * 5))
    assert rf.in_senders == 8 and rf.in_count == 8 and rf.in_sum == 16000.0
    late = fs.update_and_read(mk(99, "S0", "COLLECTOR", 2000.0, 400))   # old ones fell out of the window
    assert late.in_count == 1 and late.in_senders == 1


def test_structuring_counter():
    fs = store()
    rfs = [fs.update_and_read(mk(i, "A", "B", 9500.0, i * 30)) for i in range(4)]
    assert [r.near_count for r in rfs] == [1, 2, 3, 4] and rfs[0].is_near_threshold
    other = fs.update_and_read(mk(10, "A", "B", 500.0, 150))
    assert not other.is_near_threshold and other.near_count == 4


def test_velocity_and_idempotent_replay():
    fs = store()
    for i in range(5):
        fs.update_and_read(mk(i, "A", f"D{i}", 100.0, i))
    again = fs.update_and_read(mk(4, "A", "D4", 100.0, 4))     # same txn replayed from Kafka
    assert again.out_count == 5


def test_recent_inbound_for_pass_through():
    fs = store()
    fs.update_and_read(mk(1, "X", "MID", 30000.0, 0))
    rf = fs.update_and_read(mk(2, "MID", "Y", 29400.0, 8))
    assert rf.recent_inbound_to_src == [30000.0]
    late = fs.update_and_read(mk(3, "MID", "Z", 29000.0, 60))
    assert late.recent_inbound_to_src == []


def test_network_flags_read_and_staleness():
    import json
    r = fakeredis.FakeRedis(decode_responses=True)
    fs = FeatureStore(r, net_max_age_s=240.0)
    ts0 = T0.timestamp()
    r.set("net:A", json.dumps({"ts": ts0 - 60, "c": True, "t": True, "h": False, "cl": "CL-A", "cs": 4, "scc": 4}))
    r.set("net:B", json.dumps({"ts": ts0 - 1000, "c": True, "t": False, "h": False, "cl": "CL-B", "cs": 3, "scc": 3}))
    rf = fs.update_and_read(mk(1, "A", "B", 9000.0, 0))
    assert rf.src_net and rf.src_net["cl"] == "CL-A"
    assert rf.dst_net is None                       # analytics older than net_max_age_s -> ignored
    off = FeatureStore(r, read_net=False).update_and_read(mk(2, "A", "B", 9000.0, 1))
    assert off.src_net is None and off.dst_net is None
