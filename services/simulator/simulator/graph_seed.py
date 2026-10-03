"""Idempotently seed customers/accounts/devices/addresses/jurisdictions into Neo4j.

Only structural facts go in. Ground-truth segments (shell/mule) are deliberately omitted.
"""
from __future__ import annotations

import logging
import time

from neo4j import Driver

from .population import Population

log = logging.getLogger("simulator.seed")

CUSTOMER_Q = """
UNWIND $rows AS r
MERGE (c:Customer {customer_id: r.customer_id})
  SET c.name = r.name, c.kind = r.kind
MERGE (j:Jurisdiction {code: r.country})
MERGE (a:Address {address_id: r.address_id})
MERGE (d:Device {device_id: r.device_id})
MERGE (c)-[:RESIDENT_IN]->(j)
MERGE (c)-[:LIVES_AT]->(a)
MERGE (c)-[:USES_DEVICE]->(d)
"""

ACCOUNT_Q = """
UNWIND $rows AS r
MATCH (c:Customer {customer_id: r.customer_id})
MERGE (a:Account {account_id: r.account_id})
  SET a.country = r.country
MERGE (c)-[:OWNS]->(a)
"""

JURISDICTION_Q = "MATCH (j:Jurisdiction) SET j.high_risk = j.code IN $codes"


def _chunks(rows: list[dict], size: int):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def wait_for_neo4j(driver: Driver, attempts: int = 30, delay_s: float = 2.0) -> None:
    for i in range(attempts):
        try:
            driver.verify_connectivity()
            return
        except Exception as exc:  # noqa: BLE001
            log.warning("neo4j not ready (%s/%s): %s", i + 1, attempts, exc)
            time.sleep(delay_s)
    raise RuntimeError("Neo4j did not become available")


def seed_graph(driver: Driver, pop: Population, high_risk: list[str], batch: int = 500) -> None:
    customers = [dict(customer_id=c.customer_id, name=c.name, kind=c.kind, country=c.country,
                      address_id=c.address_id, device_id=c.device_id) for c in pop.customers]
    accounts = [dict(account_id=a.account_id, customer_id=a.customer_id, country=a.country)
                for a in pop.accounts]
    with driver.session() as session:
        for chunk in _chunks(customers, batch):
            session.execute_write(lambda tx, rows=chunk: tx.run(CUSTOMER_Q, rows=rows).consume())
        for chunk in _chunks(accounts, batch):
            session.execute_write(lambda tx, rows=chunk: tx.run(ACCOUNT_Q, rows=rows).consume())
        session.execute_write(lambda tx: tx.run(JURISDICTION_Q, codes=high_risk).consume())
    log.info("graph seeded: %s customers, %s accounts", len(customers), len(accounts))
