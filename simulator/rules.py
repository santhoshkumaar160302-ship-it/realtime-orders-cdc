"""Business rules for the simulated store (pure functions, unit-tested)."""

import random
from decimal import ROUND_HALF_UP, Decimal

TERMINAL = {"delivered", "cancelled"}
FORWARD = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}

# Weighted mix of what happens on each tick.
ACTIONS: dict[str, int] = {
    "new_order": 45,
    "advance_order": 35,
    "update_customer": 8,
    "new_customer": 7,
    "reprice_product": 3,
    "purge_cancelled": 2,
}


def next_status(current: str, rng: random.Random, cancel_rate: float = 0.1) -> str | None:
    """Return the next status, or None if the order is already final.

    Orders can only be cancelled before they ship.
    """
    if current in TERMINAL:
        return None
    if current in ("placed", "paid") and rng.random() < cancel_rate:
        return "cancelled"
    return FORWARD[current]


def order_total(items: list[tuple[int, Decimal]]) -> Decimal:
    """Sum of quantity * unit_price, rounded to cents."""
    total = sum((Decimal(q) * p for q, p in items), Decimal("0"))
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def pick_action(rng: random.Random) -> str:
    names = list(ACTIONS)
    return rng.choices(names, weights=[ACTIONS[n] for n in names], k=1)[0]
