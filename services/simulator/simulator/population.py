"""Synthetic customer / account population.

`segment` is GROUND TRUTH used only to drive scenarios. It is never written to the graph:
shell-like or mule-like behaviour must be *discovered* from structure (shared addresses,
shared devices, jurisdictions, flows) - exactly what the detection pipeline has to do.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field

NORMAL_COUNTRIES = ["IN", "US", "GB", "AE", "SG", "DE"]
SHELL_COUNTRIES = ["AE", "SG", "GB", "HK"]


@dataclass(frozen=True)
class Customer:
    customer_id: str
    name: str
    kind: str        # person | organization   (visible to the graph)
    country: str
    address_id: str
    device_id: str
    segment: str     # person | org | shell | mule   (ground truth, hidden)


@dataclass(frozen=True)
class Account:
    account_id: str
    customer_id: str
    country: str


@dataclass
class Population:
    customers: list[Customer]
    accounts: list[Account]
    high_risk: list[str]
    customer_by_id: dict[str, Customer] = field(init=False, repr=False)
    account_by_id: dict[str, Account] = field(init=False, repr=False)
    _segment_accounts: dict[str, list[Account]] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.customer_by_id = {c.customer_id: c for c in self.customers}
        self.account_by_id = {a.account_id: a for a in self.accounts}
        seg: dict[str, list[Account]] = {}
        for a in self.accounts:
            seg.setdefault(self.customer_by_id[a.customer_id].segment, []).append(a)
        self._segment_accounts = seg

    def accounts_in(self, *segments: str) -> list[Account]:
        out: list[Account] = []
        for s in segments:
            out.extend(self._segment_accounts.get(s, []))
        return out

    def customer_of(self, account_id: str) -> Customer:
        return self.customer_by_id[self.account_by_id[account_id].customer_id]


def build_population(rng: random.Random, n_customers: int, high_risk: list[str]) -> Population:
    n = max(n_customers, 100)
    n_shell = max(6, int(n * 0.04))
    n_mule = max(12, int(n * 0.04))
    n_org = int(n * 0.08)
    n_person = n - n_shell - n_mule - n_org

    shell_addresses = [f"ADDR-SHELL-{i:03d}" for i in range(max(2, n_shell // 3))]
    mule_devices = [f"DEV-RING-{i:03d}" for i in range(max(3, n_mule // 4))]

    customers: list[Customer] = []
    accounts: list[Account] = []

    def add(segment: str, kind: str, country: str, address_id: str, device_id: str, n_acc: int = 1) -> None:
        cid = f"CUS-{len(customers) + 1:06d}"
        customers.append(Customer(cid, f"Customer {len(customers) + 1}", kind, country,
                                  address_id, device_id, segment))
        for _ in range(n_acc):
            accounts.append(Account(f"ACC-{len(accounts) + 1:06d}", cid, country))

    for _ in range(n_person):
        k = len(customers) + 1
        add("person", "person", rng.choice(NORMAL_COUNTRIES), f"ADDR-{k:06d}", f"DEV-{k:06d}",
            2 if rng.random() < 0.3 else 1)
    for _ in range(n_org):
        k = len(customers) + 1
        add("org", "organization", rng.choice(NORMAL_COUNTRIES), f"ADDR-{k:06d}", f"DEV-{k:06d}",
            2 if rng.random() < 0.3 else 1)
    for i in range(n_shell):
        k = len(customers) + 1
        hr = bool(high_risk) and (i < 2 or rng.random() < 0.35)
        country = rng.choice(high_risk) if hr else rng.choice(SHELL_COUNTRIES)
        add("shell", "organization", country, rng.choice(shell_addresses), f"DEV-{k:06d}")
    for _ in range(n_mule):
        k = len(customers) + 1
        device = rng.choice(mule_devices) if rng.random() < 0.7 else f"DEV-{k:06d}"
        add("mule", "person", rng.choice(NORMAL_COUNTRIES), f"ADDR-{k:06d}", device)

    return Population(customers, accounts, list(high_risk))
