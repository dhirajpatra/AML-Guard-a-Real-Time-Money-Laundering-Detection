"""Neo4j access for the hot path: ontology, jurisdictions, account profiles, flow tracing, storage.

Graph model written here:
  (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)      full provenance
  (Account)-[f:TRANSFERRED_TO {count,total,first_ts,last_ts,last_amount}]->(Account)   aggregated flow edge

Flow tracing = paths of TRANSFERRED_TO edges that are *flow-conserving*: each hop is later than the
previous one, within the time window, and carries 80-102% of the previous hop's amount. Requiring
conserved amounts (not just connectivity) is what separates layering from ordinary payment noise.
"""
from __future__ import annotations

import logging
import time

from neo4j import Driver

from aml_common.models import Transaction

from .ontology import Typology
from .types import AccountProfile, GraphFeatures

log = logging.getLogger("processor.graph")

_FLOW = """
all(r IN relationships(p) WHERE r.last_ts >= $since)
AND all(i IN range(0, size(relationships(p)) - 2) WHERE
      relationships(p)[i + 1].last_amount <= relationships(p)[i].last_amount * 1.02
  AND relationships(p)[i + 1].last_amount >= relationships(p)[i].last_amount * 0.80
  AND relationships(p)[i + 1].last_ts >= relationships(p)[i].last_ts)
AND last(relationships(p)).last_amount * 1.02 >= $amount
AND last(relationships(p)).last_amount * 0.80 <= $amount
AND last(relationships(p)).last_ts <= $epoch
"""

# A path dst -> ... -> src of 2-3 hops that conserves flow closes a ring together with this txn (src -> dst).
CYCLE_Q = f"""
MATCH p = (d:Account {{account_id: $dst}})-[:TRANSFERRED_TO*2..3]->(s:Account {{account_id: $src}})
WHERE {_FLOW}
RETURN count(p) AS n, max(length(p)) AS max_len
"""

# A flow-conserving chain of 2-3 hops ending at the sender: this txn extends it to 3-4 hops.
CHAIN_Q = f"""
MATCH p = (u:Account)-[:TRANSFERRED_TO*2..3]->(s:Account {{account_id: $src}})
WHERE {_FLOW}
RETURN max(length(p)) AS up_len
"""

STORE_Q = """
OPTIONAL MATCH (ex:Transaction {txn_id: $txn_id})
WITH ex IS NULL AS is_new
MERGE (s:Account {account_id: $src}) ON CREATE SET s.country = $src_country
MERGE (d:Account {account_id: $dst}) ON CREATE SET d.country = $dst_country
MERGE (t:Transaction {txn_id: $txn_id})
  ON CREATE SET t.ts = datetime($ts), t.epoch = $epoch, t.amount = $amount, t.currency = $currency,
                t.channel = $channel, t.device_id = $device, t.ip = $ip
MERGE (s)-[:SENT]->(t)
MERGE (t)-[:RECEIVED_BY]->(d)
MERGE (s)-[f:TRANSFERRED_TO]->(d)
  ON CREATE SET f.count = 0, f.total = 0.0, f.first_ts = $epoch
SET f.last_ts = $epoch, f.last_amount = $amount,
    f.count = f.count + CASE WHEN is_new THEN 1 ELSE 0 END,
    f.total = f.total + CASE WHEN is_new THEN $amount ELSE 0.0 END
"""

PROFILE_Q = """
MATCH (a:Account {account_id: $acct})<-[:OWNS]-(c:Customer)
OPTIONAL MATCH (c)-[:LIVES_AT]->(:Address)<-[:LIVES_AT]-(o1:Customer) WHERE o1 <> c
OPTIONAL MATCH (c)-[:USES_DEVICE]->(:Device)<-[:USES_DEVICE]-(o2:Customer) WHERE o2 <> c
RETURN c.kind AS kind, count(DISTINCT o1) AS addr_peers, count(DISTINCT o2) AS dev_peers
"""

ONTOLOGY_Q = """
MATCH (t:Typology)-[:INDICATED_BY]->(i:RiskIndicator)
RETURN t.id AS id, t.name AS name, t.base_weight AS w, collect(i.id) AS indicators
"""

HIGH_RISK_Q = "MATCH (j:Jurisdiction {high_risk: true}) RETURN collect(j.code) AS codes"


class GraphStore:
    def __init__(self, driver: Driver, profile_ttl_s: float = 300.0, jurisdiction_ttl_s: float = 60.0) -> None:
        self.driver = driver
        self.profile_ttl_s, self.jurisdiction_ttl_s = profile_ttl_s, jurisdiction_ttl_s
        self._profiles: dict[str, tuple[float, AccountProfile]] = {}
        self._hr: tuple[float, frozenset[str]] = (0.0, frozenset())

    def wait_ready(self, attempts: int = 30, delay_s: float = 2.0) -> None:
        for i in range(attempts):
            try:
                self.driver.verify_connectivity()
                return
            except Exception as exc:  # noqa: BLE001
                log.warning("neo4j not ready (%s/%s): %s", i + 1, attempts, exc)
                time.sleep(delay_s)
        raise RuntimeError("Neo4j did not become available")

    def load_ontology(self) -> dict[str, Typology]:
        with self.driver.session() as s:
            rows = s.execute_read(lambda tx: tx.run(ONTOLOGY_Q).data())
        return {r["id"]: Typology(r["id"], r["name"], float(r["w"]), tuple(sorted(r["indicators"])))
                for r in rows}

    def high_risk_countries(self) -> frozenset[str]:
        now = time.monotonic()
        if now >= self._hr[0]:
            with self.driver.session() as s:
                rec = s.execute_read(lambda tx: tx.run(HIGH_RISK_Q).single())
            self._hr = (now + self.jurisdiction_ttl_s, frozenset(rec["codes"] if rec else []))
        return self._hr[1]

    def profile(self, account_id: str) -> AccountProfile:
        now = time.monotonic()
        hit = self._profiles.get(account_id)
        if hit and hit[0] > now:
            return hit[1]
        with self.driver.session() as s:
            rec = s.execute_read(lambda tx: tx.run(PROFILE_Q, acct=account_id).single())
        if rec is None:   # not seeded yet -> short negative cache so we pick it up soon
            prof, ttl = AccountProfile(), 5.0
        else:
            prof, ttl = AccountProfile(rec["kind"], int(rec["addr_peers"]), int(rec["dev_peers"])), self.profile_ttl_s
        self._profiles[account_id] = (now + ttl, prof)
        return prof

    def analyze_and_store(self, txn: Transaction, since: float, run_flow: bool) -> GraphFeatures:
        params = dict(
            txn_id=txn.txn_id, ts=txn.ts.isoformat(), epoch=txn.ts.timestamp(), since=since,
            src=txn.src_account, dst=txn.dst_account, amount=txn.amount, currency=txn.currency,
            channel=txn.channel, device=txn.device_id, ip=txn.ip,
            src_country=txn.src_country, dst_country=txn.dst_country,
        )

        def work(tx) -> GraphFeatures:
            cycle_len, up = None, 0
            if run_flow:
                rec = tx.run(CYCLE_Q, **params).single()
                if rec and rec["n"]:
                    cycle_len = int(rec["max_len"]) + 1
                rec = tx.run(CHAIN_Q, **params).single()
                if rec and rec["up_len"]:
                    up = int(rec["up_len"])
            tx.run(STORE_Q, **params).consume()
            return GraphFeatures(cycle_len, up)

        with self.driver.session() as s:
            return s.execute_write(work)
