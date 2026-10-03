"""Pure (side-effect free) laundering scenario generators - easy to unit test.

Each scenario is a list of PlannedTxn with a delay (seconds from scenario start).
Typology ids match the :Typology nodes in graph/01_schema.cypher.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

from .population import Population


@dataclass(frozen=True)
class PlannedTxn:
    delay_s: float
    src_account: str
    dst_account: str
    amount: float
    channel: str
    narrative: str = ""


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    typology: str
    txns: list[PlannedTxn]


def _cumulative(rng: random.Random, n: int, lo: float, hi: float) -> list[float]:
    t, out = 0.0, []
    for _ in range(n):
        out.append(round(t, 2))
        t += rng.uniform(lo, hi)
    return out


def structuring(rng: random.Random, pop: Population) -> list[PlannedTxn]:
    src = rng.choice(pop.accounts_in("mule", "person"))
    dst = rng.choice([a for a in pop.accounts_in("shell", "org") if a.account_id != src.account_id])
    n = rng.randint(5, 9)
    delays = _cumulative(rng, n, 5, 40)
    return [PlannedTxn(d, src.account_id, dst.account_id, round(rng.uniform(9000, 9900), 2),
                       "branch_cash", "cash deposit") for d in delays]


def fan_in(rng: random.Random, pop: Population) -> list[PlannedTxn]:
    collector = rng.choice(pop.accounts_in("shell", "org"))
    mules = [a for a in pop.accounts_in("mule") if a.account_id != collector.account_id]
    senders = rng.sample(mules, min(len(mules), rng.randint(8, 12)))
    delays = sorted(rng.uniform(0, 60) for _ in senders)
    return [PlannedTxn(round(d, 2), s.account_id, collector.account_id,
                       round(rng.uniform(1500, 4000), 2), "wire", "invoice payment")
            for s, d in zip(senders, delays)]


def round_trip(rng: random.Random, pop: Population) -> list[PlannedTxn]:
    ring = rng.sample(pop.accounts_in("shell", "org"), rng.randint(3, 4))
    amount = rng.uniform(20000, 90000)
    delays = _cumulative(rng, len(ring), 8, 25)
    out = []
    for i, d in enumerate(delays):
        out.append(PlannedTxn(d, ring[i].account_id, ring[(i + 1) % len(ring)].account_id,
                              round(amount, 2), "wire", "consulting fee"))
        amount *= rng.uniform(0.97, 0.995)
    return out


def rapid_pass_through(rng: random.Random, pop: Population) -> list[PlannedTxn]:
    src = rng.choice(pop.accounts_in("mule", "person"))
    shells = pop.accounts_in("shell")
    mid = rng.choice(shells)
    others = [a for a in shells if a.account_id != mid.account_id and a.country != mid.country] \
        or [a for a in shells if a.account_id != mid.account_id]
    dst = rng.choice(others)
    amount = rng.uniform(15000, 60000)
    return [
        PlannedTxn(0.0, src.account_id, mid.account_id, round(amount, 2), "wire", "loan repayment"),
        PlannedTxn(round(rng.uniform(3, 15), 2), mid.account_id, dst.account_id,
                   round(amount * rng.uniform(0.97, 0.99), 2), "wire", "service fee"),
    ]


def shell_layering(rng: random.Random, pop: Population) -> list[PlannedTxn]:
    shells = pop.accounts_in("shell")
    hr = [a for a in shells if a.country in pop.high_risk] or shells
    last = rng.choice(hr)
    rest = [a for a in shells if a.account_id != last.account_id]
    chain = rng.sample(rest, rng.randint(3, 4)) + [last]
    amount = rng.uniform(50000, 150000)
    delays = _cumulative(rng, len(chain) - 1, 10, 40)
    out = []
    for i, d in enumerate(delays):
        out.append(PlannedTxn(d, chain[i].account_id, chain[i + 1].account_id,
                              round(amount, 2), "wire", "trade settlement"))
        amount *= rng.uniform(0.95, 0.99)
    return out


GENERATORS = {
    "STRUCTURING": structuring,
    "FAN_IN": fan_in,
    "ROUND_TRIP": round_trip,
    "RAPID_PASS_THROUGH": rapid_pass_through,
    "SHELL_LAYERING": shell_layering,
}


def make_scenario(rng: random.Random, pop: Population, typology: str | None = None) -> Scenario:
    typology = typology or rng.choice(sorted(GENERATORS))
    return Scenario(f"SCN-{rng.getrandbits(32):08x}", typology, GENERATORS[typology](rng, pop))


CHANNELS = ["card", "upi", "wire", "ach", "mobile"]
CHANNEL_WEIGHTS = [40, 25, 10, 15, 10]


def normal_txn(rng: random.Random, accounts: list) -> PlannedTxn:
    """Ordinary customer payment: log-normal amounts (median ~400), truncated at 50k by resampling."""
    src, dst = rng.sample(accounts, 2)
    amount = rng.lognormvariate(6.0, 1.0)
    while amount > 50000:
        amount = rng.lognormvariate(6.0, 1.0)
    return PlannedTxn(0.0, src.account_id, dst.account_id, round(max(amount, 1.0), 2),
                      rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0], "payment")
