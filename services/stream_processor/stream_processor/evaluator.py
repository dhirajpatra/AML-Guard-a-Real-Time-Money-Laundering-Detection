"""Benchmark: join decisions with ground truth -> detection quality + latency percentiles.

    docker compose run --rm stream-processor python -m stream_processor.evaluator [--idle 10] [--max-decisions N]

Reads both topics from the beginning, so run it on a clean stack (`make reset`) after a finite simulator run.
"""
from __future__ import annotations

import argparse
import json
import time
import uuid

from confluent_kafka import Consumer

from aml_common.config import Settings

from .stats import summarize


def drain(bootstrap: str, topic: str, idle_s: float, max_msgs: int = 0) -> list[dict]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"eval-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out: list[dict] = []
    start = last = time.monotonic()
    while True:
        msg = c.poll(1.0)
        now = time.monotonic()
        if msg is None or msg.error():
            if now - (last if out else start) > (idle_s if out else idle_s + 20):
                break
            continue
        last = now
        out.append(json.loads(msg.value()))
        if max_msgs and len(out) >= max_msgs:
            break
    c.close()
    return out


def evaluate(decisions: list[dict], truth_rows: list[dict]) -> dict:
    dec = {d["txn_id"]: d for d in decisions}
    truth = {t["txn_id"]: t for t in truth_rows}
    joined = [(dec[i], truth[i]) for i in dec if i in truth]
    flagged = lambda d: d["verdict"] != "PASS"  # noqa: E731
    tp = sum(1 for d, t in joined if t["is_laundering"] and flagged(d))
    fn = sum(1 for d, t in joined if t["is_laundering"] and not flagged(d))
    fp = sum(1 for d, t in joined if not t["is_laundering"] and flagged(d))
    tn = sum(1 for d, t in joined if not t["is_laundering"] and not flagged(d))
    div = lambda a, b: round(a / b, 4) if b else 0.0  # noqa: E731

    scen: dict[str, dict] = {}
    for d, t in sorted((x for x in joined if x[1]["is_laundering"]), key=lambda x: x[0]["decided_at"]):
        s = scen.setdefault(t["scenario_id"], {"typology": t["typology"], "n": 0, "hit": 0, "block": 0,
                                               "first": None})
        s["n"] += 1
        if flagged(d):
            s["hit"] += 1
            if s["first"] is None:
                s["first"] = s["n"]            # 1-based position of the first flagged txn in the scenario
        s["block"] += d["verdict"] == "BLOCK"
    by_typ: dict[str, dict] = {}
    for s in scen.values():
        b = by_typ.setdefault(s["typology"], {"scenarios": 0, "detected": 0, "early": 0, "txns": 0,
                                              "txns_flagged": 0})
        b["scenarios"] += 1
        b["detected"] += s["hit"] > 0
        b["early"] += s["first"] is not None and s["first"] < s["n"]   # flagged before the pattern completed
        b["txns"] += s["n"]
        b["txns_flagged"] += s["hit"]

    return {
        "joined": len(joined), "unmatched_decisions": len(dec) - len(joined),
        "transactions": {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
                         "precision": div(tp, tp + fp), "recall": div(tp, tp + fn), "fpr": div(fp, fp + tn)},
        "scenarios": {"total": len(scen), "detected": sum(1 for s in scen.values() if s["hit"] > 0),
                      "detection_rate": div(sum(1 for s in scen.values() if s["hit"] > 0), len(scen)),
                      "early_rate": div(sum(1 for s in scen.values() if s["first"] and s["first"] < s["n"]),
                                        len(scen))},
        "by_typology": by_typ,
        "latency_ms": {"processing": summarize([d["processing_ms"] for d, _ in joined]),
                       "end_to_end": summarize([d["e2e_ms"] for d, _ in joined])},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--idle", type=float, default=10.0, help="stop after N seconds without new messages")
    ap.add_argument("--max-decisions", type=int, default=0)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    s = Settings()
    decisions = drain(s.kafka_bootstrap, s.topic_decisions, a.idle, a.max_decisions)
    truth = drain(s.kafka_bootstrap, s.topic_truth, a.idle)
    r = evaluate(decisions, truth)
    if a.json:
        print(json.dumps(r, indent=2))
        return
    t, sc, lat = r["transactions"], r["scenarios"], r["latency_ms"]
    print(f"\nEvaluated {r['joined']} transactions ({r['unmatched_decisions']} decisions without truth)")
    print(f"  Txn level : precision {t['precision']:.3f}  recall {t['recall']:.3f}  FPR {t['fpr']:.4%}"
          f"   (TP {t['tp']} FP {t['fp']} FN {t['fn']} TN {t['tn']})")
    print(f"  Scenarios : {sc['detected']}/{sc['total']} detected ({sc['detection_rate']:.1%})  "
          f"- a scenario counts as detected if any of its txns is flagged")
    print(f"  Early     : {sc['early_rate']:.1%} of scenarios flagged BEFORE their final transaction")
    print("\n  Typology               scenarios  detected  early  txns flagged")
    for k, b in sorted(r["by_typology"].items()):
        print(f"  {k:<22} {b['scenarios']:>9} {b['detected']:>9} {b['early']:>6} {b['txns_flagged']:>7}/{b['txns']}")
    for name, v in lat.items():
        print(f"\n  Latency {name:<11} p50 {v['p50']:>8.1f} ms  p95 {v['p95']:>8.1f} ms  "
              f"p99 {v['p99']:>8.1f} ms  max {v['max']:>8.1f} ms  (n={v['n']})")
    print()


if __name__ == "__main__":
    main()
