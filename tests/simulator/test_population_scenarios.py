import random

import pytest

from simulator.population import build_population
from simulator.scenarios import GENERATORS, make_scenario

HR = ["IR", "KP", "MM"]


@pytest.fixture
def pop():
    return build_population(random.Random(7), 300, HR)


def test_population_deterministic():
    a = build_population(random.Random(1), 200, HR)
    b = build_population(random.Random(1), 200, HR)
    assert [c.customer_id for c in a.customers] == [c.customer_id for c in b.customers]
    assert [x.country for x in a.accounts] == [x.country for x in b.accounts]


def test_population_has_structure_for_detection(pop):
    shells = pop.accounts_in("shell")
    assert len([a for a in shells if a.country in HR]) >= 2
    addrs = [pop.customer_of(a.account_id).address_id for a in shells]
    assert len(set(addrs)) < len(addrs), "shells should share registered addresses"
    devs = [pop.customer_of(a.account_id).device_id for a in pop.accounts_in("mule")]
    assert len(set(devs)) < len(devs), "mules should share devices"


@pytest.mark.parametrize("typology", sorted(GENERATORS))
def test_every_scenario_is_well_formed(pop, typology):
    for seed in range(25):
        sc = make_scenario(random.Random(seed), pop, typology)
        assert sc.typology == typology and sc.txns
        for t in sc.txns:
            assert t.amount > 0 and t.delay_s >= 0
            assert t.src_account != t.dst_account
            assert t.src_account in pop.account_by_id and t.dst_account in pop.account_by_id


def test_structuring_just_below_threshold(pop):
    sc = make_scenario(random.Random(3), pop, "STRUCTURING")
    assert 5 <= len(sc.txns) <= 9
    assert all(9000 <= t.amount <= 9900 for t in sc.txns)
    assert len({(t.src_account, t.dst_account) for t in sc.txns}) == 1


def test_fan_in_many_senders_one_collector(pop):
    sc = make_scenario(random.Random(4), pop, "FAN_IN")
    assert len({t.src_account for t in sc.txns}) >= 8
    assert len({t.dst_account for t in sc.txns}) == 1


def test_round_trip_returns_to_origin(pop):
    for seed in range(20):
        sc = make_scenario(random.Random(seed), pop, "ROUND_TRIP")
        assert sc.txns[-1].dst_account == sc.txns[0].src_account
        for a, b in zip(sc.txns, sc.txns[1:]):
            assert a.dst_account == b.src_account


def test_rapid_pass_through_is_fast_and_loses_a_little(pop):
    sc = make_scenario(random.Random(5), pop, "RAPID_PASS_THROUGH")
    first, second = sc.txns
    assert second.src_account == first.dst_account
    assert second.delay_s - first.delay_s <= 15
    assert 0.96 * first.amount <= second.amount < first.amount


def test_shell_layering_chain_ends_in_high_risk(pop):
    for seed in range(20):
        sc = make_scenario(random.Random(seed), pop, "SHELL_LAYERING")
        assert pop.account_by_id[sc.txns[-1].dst_account].country in HR
        for a, b in zip(sc.txns, sc.txns[1:]):
            assert a.dst_account == b.src_account
