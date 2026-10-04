"""Stream processor: transactions.raw -> hot-path scoring -> transactions.decisions / alerts (+ Postgres)."""
from __future__ import annotations

import logging
import signal
import time

import redis
from confluent_kafka import Consumer, Producer
from neo4j import GraphDatabase
from pydantic import ValidationError

from aml_common.logging_setup import setup_logging
from aml_common.models import Decision, Transaction

from .features import FeatureStore
from .graph import GraphStore
from .ontology import DEFAULT_ONTOLOGY
from .persistence import PgWriter
from .pipeline import Pipeline
from .rules import RuleConfig
from .settings import ProcSettings
from .stats import LatencyWindow

log = logging.getLogger("processor")


def run() -> None:
    s = ProcSettings()
    setup_logging(s.log_level)

    driver = GraphDatabase.driver(s.neo4j_uri, auth=(s.neo4j_user, s.neo4j_password))
    graph = GraphStore(driver, profile_ttl_s=s.profile_ttl_s)
    graph.wait_ready()
    ontology = graph.load_ontology()
    if not ontology:
        log.warning("no typologies found in graph - using built-in defaults")
        ontology = DEFAULT_ONTOLOGY
    log.info("ontology loaded", extra={"ctx": {"typologies": sorted(ontology)}})

    rds = redis.Redis.from_url(s.redis_url, decode_responses=True)
    rds.ping()
    pg = PgWriter(s.postgres_dsn)
    pg.connect()
    pg.ensure_schema()

    cfg = RuleConfig(review_threshold=s.review_threshold, block_threshold=s.block_threshold,
                     flow_min_amount=s.flow_min_amount, use_network=s.net_features_enabled,
                     net_min_amount=s.flow_min_amount)
    features = FeatureStore(rds, read_net=s.net_features_enabled, net_max_age_s=s.net_max_age_s)
    pipeline = Pipeline(features, graph, ontology, cfg, flow_window_s=s.flow_window_s)

    consumer = Consumer({
        "bootstrap.servers": s.kafka_bootstrap, "group.id": s.proc_group,
        "auto.offset.reset": s.proc_offset_reset, "enable.auto.commit": False,
    })
    consumer.subscribe([s.topic_raw])
    producer = Producer({"bootstrap.servers": s.kafka_bootstrap, "client.id": "aml-stream-processor",
                         "linger.ms": 5})

    stop = {"flag": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.__setitem__("flag", True))

    proc_lat, e2e_lat = LatencyWindow(), LatencyWindow()
    counts = {"txns": 0, "review": 0, "block": 0, "dlq": 0}
    pending: list[Decision] = []
    last_flush = last_stats = time.monotonic()
    last_stats_txns = 0

    def flush() -> None:
        nonlocal last_flush
        producer.flush(5)          # decisions are on the wire before offsets are committed
        pg.write(pending)
        consumer.commit(asynchronous=False)
        pending.clear()
        last_flush = time.monotonic()

    log.info("stream processor started", extra={"ctx": {"group": s.proc_group, "topic": s.topic_raw}})
    while not stop["flag"]:
        msg = consumer.poll(0.05)
        if msg is not None and not msg.error():
            try:
                txn = Transaction.model_validate_json(msg.value())
                d = pipeline.process(txn)
            except (ValidationError, ValueError) as exc:
                counts["dlq"] += 1
                producer.produce(s.topic_dlq, key=msg.key(), value=msg.value(),
                                 headers={"error": str(exc)[:200]})
                log.warning("bad message sent to DLQ: %s", str(exc)[:200])
            else:
                body = d.model_dump_json().encode()
                producer.produce(s.topic_decisions, key=d.txn_id.encode(), value=body)
                if d.verdict != "PASS":
                    producer.produce(s.topic_alerts, key=d.txn_id.encode(), value=body)
                    counts["block" if d.verdict == "BLOCK" else "review"] += 1
                producer.poll(0)
                pending.append(d)
                counts["txns"] += 1
                proc_lat.add(d.processing_ms)
                e2e_lat.add(d.e2e_ms)
        elif msg is not None and msg.error():
            log.error("kafka error: %s", msg.error())

        now = time.monotonic()
        if pending and (len(pending) >= s.proc_batch or msg is None
                        or now - last_flush >= s.proc_flush_interval_s):
            flush()
        if now - last_stats >= s.stats_interval_s:
            tps = (counts["txns"] - last_stats_txns) / (now - last_stats)
            log.info("stats", extra={"ctx": {**counts, "tps": round(tps, 1),
                                              "processing_ms": proc_lat.summary(), "e2e_ms": e2e_lat.summary()}})
            last_stats, last_stats_txns = now, counts["txns"]

    if pending:
        flush()
    consumer.close()
    driver.close()
    log.info("stream processor stopped", extra={"ctx": counts})


if __name__ == "__main__":
    run()
