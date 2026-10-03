"""Typology definitions. Source of truth is the :Typology graph (graph/01_schema.cypher);
this fallback is verified against that file by tests/stream_processor/test_rules.py."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Typology:
    id: str
    name: str
    base_weight: float
    indicators: tuple[str, ...]


DEFAULT_ONTOLOGY: dict[str, Typology] = {t.id: t for t in [
    Typology("STRUCTURING", "Structuring", 0.70, ("JUST_BELOW_THRESHOLD", "HIGH_VELOCITY")),
    Typology("FAN_IN", "Smurfing / Fan-in", 0.75, ("MANY_TO_ONE", "SHARED_DEVICE", "HIGH_VELOCITY")),
    Typology("ROUND_TRIP", "Round-tripping", 0.85, ("CYCLE", "LAYERED_HOPS")),
    Typology("RAPID_PASS_THROUGH", "Rapid pass-through", 0.80, ("RAPID_PASS_THROUGH", "HIGH_RISK_JURISDICTION")),
    Typology("SHELL_LAYERING", "Shell-company layering", 0.90,
             ("LAYERED_HOPS", "SHARED_ADDRESS", "HIGH_RISK_JURISDICTION")),
]}
