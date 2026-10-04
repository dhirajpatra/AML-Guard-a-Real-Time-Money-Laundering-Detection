from stream_processor.evaluator import evaluate


def dec(i, verdict, ts):
    return dict(txn_id=f"T{i}", verdict=verdict, processing_ms=2.0, e2e_ms=5.0, decided_at=ts)


def test_confusion_scenarios_and_early_detection():
    decisions = [dec(0, "PASS", "2026-01-01T00:00:01Z"), dec(1, "REVIEW", "2026-01-01T00:00:02Z"),
                 dec(2, "BLOCK", "2026-01-01T00:00:03Z"),      # scenario S1: flagged at 2nd of 3 -> early
                 dec(3, "PASS", "2026-01-01T00:00:01Z"), dec(4, "REVIEW", "2026-01-01T00:00:02Z"),  # S2: last only
                 dec(5, "PASS", "2026-01-01T00:00:03Z"), dec(6, "REVIEW", "2026-01-01T00:00:04Z")]  # normals
    truth = [dict(txn_id="T0", is_laundering=True, typology="FAN_IN", scenario_id="S1"),
             dict(txn_id="T1", is_laundering=True, typology="FAN_IN", scenario_id="S1"),
             dict(txn_id="T2", is_laundering=True, typology="FAN_IN", scenario_id="S1"),
             dict(txn_id="T3", is_laundering=True, typology="ROUND_TRIP", scenario_id="S2"),
             dict(txn_id="T4", is_laundering=True, typology="ROUND_TRIP", scenario_id="S2"),
             dict(txn_id="T5", is_laundering=False), dict(txn_id="T6", is_laundering=False)]
    r = evaluate(decisions, truth)
    assert r["transactions"] | {} == r["transactions"] and (r["transactions"]["tp"], r["transactions"]["fp"],
                                                            r["transactions"]["fn"]) == (3, 1, 2)
    assert r["scenarios"]["detected"] == 2 and r["scenarios"]["early_rate"] == 0.5
    assert r["by_typology"]["FAN_IN"]["early"] == 1 and r["by_typology"]["ROUND_TRIP"]["early"] == 0
