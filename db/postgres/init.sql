-- Case store: decisions, alerts, audit trail (idempotent)
CREATE TABLE IF NOT EXISTS decisions (
    txn_id      TEXT PRIMARY KEY,
    decided_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    verdict     TEXT NOT NULL,                 -- PASS | REVIEW | BLOCK
    risk_score  DOUBLE PRECISION NOT NULL,
    latency_ms  DOUBLE PRECISION,
    reasons     JSONB NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id    BIGSERIAL PRIMARY KEY,
    txn_id      TEXT NOT NULL,
    typology    TEXT,
    severity    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open',  -- open | investigating | closed
    narrative   TEXT,
    evidence    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id      BIGSERIAL PRIMARY KEY,
    ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor   TEXT NOT NULL,
    action  TEXT NOT NULL,
    txn_id  TEXT,
    detail  JSONB NOT NULL DEFAULT '{}'
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_txn ON alerts(txn_id);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
CREATE INDEX IF NOT EXISTS idx_audit_txn ON audit_log(txn_id);
