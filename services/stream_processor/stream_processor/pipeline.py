"""Hot-path orchestration for one transaction: Redis features -> graph (read, then write) -> score."""
from __future__ import annotations

import time
from datetime import datetime, timezone

from aml_common.models import Decision, Transaction
from aml_common.telemetry import NullObs

from .rules import RuleConfig, derive_indicators, score_typologies, verdict_for


class Pipeline:
    def __init__(self, features, graph, ontology, cfg: RuleConfig, flow_window_s: float = 300.0,
                 obs=None) -> None:
        self.features, self.graph, self.ontology = features, graph, ontology
        self.obs = obs or NullObs()
        self.cfg, self.flow_window_s = cfg, flow_window_s

    def process(self, txn: Transaction, now: datetime | None = None) -> Decision:
        t0 = time.perf_counter()
        epoch = txn.ts.timestamp()
        with self.obs.stage("redis_features"):
            rf = self.features.update_and_read(txn)
        # Graph: flow queries read the state BEFORE this txn is written, then the txn is stored.
        with self.obs.stage("graph_flow_store"):
            gf = self.graph.analyze_and_store(txn, since=epoch - self.flow_window_s,
                                              run_flow=txn.amount >= self.cfg.flow_min_amount)
        with self.obs.stage("graph_profiles"):
            sp, dp = self.graph.profile(txn.src_account), self.graph.profile(txn.dst_account)
            hr = self.graph.high_risk_countries()
        with self.obs.stage("score"):
            fired, evidence = derive_indicators(txn, rf, gf, sp, dp, hr, self.cfg)
            risk, typology, per = score_typologies(fired, self.ontology)
            if per:
                evidence["typology_scores"] = per
        processing_ms = (time.perf_counter() - t0) * 1000
        now = now or datetime.now(timezone.utc)
        return Decision(
            txn_id=txn.txn_id, decided_at=now, verdict=verdict_for(risk, self.cfg),
            risk_score=round(risk, 4), typology=typology, indicators=sorted(fired), evidence=evidence,
            processing_ms=round(processing_ms, 3),
            e2e_ms=round(max(0.0, (now - txn.ts).total_seconds() * 1000), 3),
            src_account=txn.src_account, dst_account=txn.dst_account, amount=txn.amount,
        )
