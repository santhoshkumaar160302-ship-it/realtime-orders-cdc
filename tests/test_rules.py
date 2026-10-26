import random
from decimal import Decimal

import pytest

from simulator.rules import ACTIONS, next_status, order_total, pick_action


def test_terminal_states_do_not_move():
    rng = random.Random(1)
    assert next_status("delivered", rng) is None
    assert next_status("cancelled", rng) is None


def test_shipped_orders_cannot_be_cancelled():
    rng = random.Random(1)
    assert all(next_status("shipped", rng, cancel_rate=1.0) == "delivered" for _ in range(50))


@pytest.mark.parametrize("start,expected", [("placed", "paid"), ("paid", "shipped")])
def test_forward_path_without_cancellations(start, expected):
    assert next_status(start, random.Random(0), cancel_rate=0.0) == expected


def test_cancellation_only_before_shipping():
    rng = random.Random(0)
    assert next_status("placed", rng, cancel_rate=1.0) == "cancelled"
    assert next_status("paid", rng, cancel_rate=1.0) == "cancelled"


def test_order_total_rounds_to_cents():
    assert order_total([(3, Decimal("19.99")), (1, Decimal("0.005"))]) == Decimal("59.98")
    assert order_total([]) == Decimal("0.00")


def test_pick_action_respects_weights():
    rng = random.Random(42)
    picks = [pick_action(rng) for _ in range(5000)]
    assert set(picks) <= set(ACTIONS)
    assert picks.count("new_order") > picks.count("purge_cancelled") * 10
