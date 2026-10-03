"""Transaction simulator: streams normal traffic plus injected laundering scenarios to Redpanda."""
from __future__ import annotations

import heapq
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from neo4j import GraphDatabase

from aml_common.logging_setup import setup_logging
from aml_common.models import GroundTruth, Transaction

from .graph_seed import seed_graph, wait_for_neo4j
from .population import Population, build_population
from .scenarios import PlannedTxn, Scenario, make_scenario, normal_txn
from .settings import SimSettings

log = logging.getLogger("simulator")


class Emitter:
    def __init__(self, s: SimSettings, pop: Population, rng: random.Random) -> None:
        self.s, self.pop, self.rng = s, pop, rng
        self.producer = Producer({
            "bootstrap.servers": s.kafka_bootstrap,
            "client.id": "aml-simulator",
            "linger.ms": 5,
        })
        self.normal_accounts = pop.accounts_in("person", "org")
        self.total = 0
        self.laundering = 0

    def _send(self, topic: str, key: str, value: str) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key.encode(), value=value.encode())
                return
            except BufferError:
                self.producer.poll(0.1)

    def emit(self, p: PlannedTxn, scenario: Scenario | None) -> None:
        src = self.pop.account_by_id[p.src_account]
        dst = self.pop.account_by_id[p.dst_account]
        cust = self.pop.customer_by_id[src.customer_id]
        r = self.rng
        txn = Transaction(
            txn_id=f"TX-{uuid.uuid4().hex[:16]}",
            ts=datetime.now(timezone.utc),
            src_account=src.account_id, dst_account=dst.account_id,
            amount=p.amount, channel=p.channel,
            src_country=src.country, dst_country=dst.country,
            device_id=cust.device_id,
            ip=f"10.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}",
            narrative=p.narrative,
        )
        truth = GroundTruth(
            txn_id=txn.txn_id, is_laundering=scenario is not None,
            typology=scenario.typology if scenario else None,
            scenario_id=scenario.scenario_id if scenario else None,
        )
        self._send(self.s.topic_raw, txn.src_account, txn.model_dump_json())
        self._send(self.s.topic_truth, txn.txn_id, truth.model_dump_json())
        self.producer.poll(0)
        self.total += 1
        self.laundering += scenario is not None

    def normal(self) -> PlannedTxn:
        return normal_txn(self.rng, self.normal_accounts)

    def flush(self) -> None:
        self.producer.flush(10)


def run() -> None:
    s = SimSettings()
    setup_logging(s.log_level)
    rng = random.Random(s.sim_seed)
    pop = build_population(rng, s.sim_customers, s.high_risk_list)
    log.info("population: %s customers, %s accounts", len(pop.customers), len(pop.accounts))

    if s.sim_seed_graph:
        driver = GraphDatabase.driver(s.neo4j_uri, auth=(s.neo4j_user, s.neo4j_password))
        try:
            wait_for_neo4j(driver)
            seed_graph(driver, pop, s.high_risk_list)
        finally:
            driver.close()

    em = Emitter(s, pop, rng)
    stop = {"flag": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.__setitem__("flag", True))

    heap: list[tuple[float, int, PlannedTxn, Scenario]] = []
    seq = 0
    interval = 1.0 / max(s.sim_tps, 0.001)
    scen_per_sec = s.sim_scenarios_per_min / 60.0
    now = time.monotonic()
    next_normal, last_tick, last_log = now, now, now
    log.info("streaming at %.1f tps, %.1f scenarios/min", s.sim_tps, s.sim_scenarios_per_min)

    while not stop["flag"]:
        now = time.monotonic()
        if rng.random() < scen_per_sec * (now - last_tick):
            sc = make_scenario(rng, pop)
            for p in sc.txns:
                seq += 1
                heapq.heappush(heap, (now + p.delay_s, seq, p, sc))
            log.info("scenario started", extra={"ctx": {"scenario": sc.scenario_id,
                                                         "typology": sc.typology, "txns": len(sc.txns)}})
        last_tick = now
        while heap and heap[0][0] <= now:
            _, _, p, sc = heapq.heappop(heap)
            em.emit(p, sc)
        if now - next_normal > 1.0:      # don't burst after a stall
            next_normal = now
        while now >= next_normal:
            em.emit(em.normal(), None)
            next_normal += interval
        if now - last_log >= 10:
            log.info("stats", extra={"ctx": {"total": em.total, "laundering": em.laundering,
                                              "pending_scenario_txns": len(heap)}})
            last_log = now
        if s.sim_max_txns and em.total >= s.sim_max_txns:
            break
        time.sleep(0.002)

    em.flush()
    log.info("simulator stopped", extra={"ctx": {"total": em.total, "laundering": em.laundering}})


if __name__ == "__main__":
    run()
