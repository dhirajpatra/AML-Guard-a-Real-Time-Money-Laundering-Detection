"""Postgres case store writer. Batched and idempotent (ON CONFLICT DO NOTHING) -> safe on Kafka replay."""
from __future__ import annotations

import logging
import time

import psycopg
from psycopg.types.json import Jsonb

from aml_common.models import Decision

log = logging.getLogger("processor.pg")

DECISION_SQL = """INSERT INTO decisions (txn_id, decided_at, verdict, risk_score, latency_ms, reasons)
VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (txn_id) DO NOTHING"""
ALERT_SQL = """INSERT INTO alerts (txn_id, typology, severity, status, narrative, evidence)
VALUES (%s, %s, %s, 'open', NULL, %s) ON CONFLICT (txn_id) DO NOTHING"""


class PgWriter:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.conn: psycopg.Connection | None = None

    def connect(self, attempts: int = 30, delay_s: float = 2.0) -> None:
        for i in range(attempts):
            try:
                self.conn = psycopg.connect(self.dsn)
                return
            except psycopg.OperationalError as exc:
                log.warning("postgres not ready (%s/%s): %s", i + 1, attempts, exc)
                time.sleep(delay_s)
        raise RuntimeError("Postgres did not become available")

    def ensure_schema(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_txn ON alerts(txn_id)")
        self.conn.commit()

    def write(self, decisions: list[Decision]) -> None:
        if not decisions:
            return
        try:
            self._write(decisions)
        except psycopg.OperationalError:
            log.warning("postgres connection lost, reconnecting")
            self.connect()
            self._write(decisions)

    def _write(self, decisions: list[Decision]) -> None:
        dec_rows, alert_rows = [], []
        for d in decisions:
            dec_rows.append((d.txn_id, d.decided_at, d.verdict, d.risk_score, d.processing_ms,
                             Jsonb({"typology": d.typology, "indicators": d.indicators,
                                    "evidence": d.evidence, "e2e_ms": d.e2e_ms})))
            if d.verdict != "PASS":
                alert_rows.append((d.txn_id, d.typology, "high" if d.verdict == "BLOCK" else "medium",
                                   Jsonb({"risk_score": d.risk_score, "indicators": d.indicators,
                                          "evidence": d.evidence, "src": d.src_account,
                                          "dst": d.dst_account, "amount": d.amount})))
        try:
            with self.conn.cursor() as cur:
                cur.executemany(DECISION_SQL, dec_rows)
                if alert_rows:
                    cur.executemany(ALERT_SQL, alert_rows)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
