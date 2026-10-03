"""Event contracts shared between services (JSON on Kafka topics)."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class Transaction(BaseModel):
    txn_id: str
    ts: datetime
    src_account: str
    dst_account: str
    amount: float = Field(gt=0)
    currency: str = "USD"
    channel: str
    src_country: str
    dst_country: str
    device_id: str | None = None
    ip: str | None = None
    narrative: str = ""


class GroundTruth(BaseModel):
    """Published by the simulator ONLY for evaluation. Detection services must never read it."""

    txn_id: str
    is_laundering: bool
    typology: str | None = None
    scenario_id: str | None = None


class Decision(BaseModel):
    """Hot-path verdict published on `transactions.decisions` (and `alerts` when not PASS)."""

    txn_id: str
    decided_at: datetime
    verdict: str                      # PASS | REVIEW | BLOCK
    risk_score: float
    typology: str | None = None       # best-matching ontology typology id
    indicators: list[str] = Field(default_factory=list)
    evidence: dict[str, Any] = Field(default_factory=dict)
    processing_ms: float              # time spent inside the hot path for this txn
    e2e_ms: float                     # event time -> verdict (includes queueing)
    src_account: str
    dst_account: str
    amount: float
