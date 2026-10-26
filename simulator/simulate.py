"""Generates realistic OLTP traffic against the `shop` Postgres database.

Inserts customers and orders, moves orders through their lifecycle,
updates customer details, reprices products and hard-deletes old cancelled
orders, so the CDC pipeline sees inserts, updates and deletes.

Env vars: EVENTS_PER_SEC (default 10), SEED (optional), MAX_EVENTS (optional,
stops after N actions; used by the end-to-end test).
"""

import os
import random
import time
from decimal import Decimal

import psycopg

from simulator.rules import next_status, order_total, pick_action

FIRST = ["Ava", "Liam", "Noah", "Emma", "Mia", "Ethan", "Priya", "Arjun", "Sofia", "Lucas",
         "Chloe", "Mateo", "Aisha", "Wei", "Hana", "Omar", "Grace", "Diego", "Zara", "Kai"]
LAST = ["Smith", "Patel", "Garcia", "Kim", "Nguyen", "Johnson", "Lee", "Brown", "Rajan",
        "Lopez", "Chen", "Davis", "Martin", "Singh", "Clark", "Wright", "Ali", "Young"]
STATES = ["IN", "CA", "TX", "NY", "IL", "WA", "FL", "OH", "GA", "MA", "CO", "AZ"]
CATEGORIES = {
    "electronics": (19.99, 899.00), "home": (8.50, 249.00), "books": (6.99, 45.00),
    "grocery": (1.99, 29.99), "toys": (4.99, 89.00), "sports": (9.99, 349.00),
}


def connect() -> psycopg.Connection:
    dsn = (
        f"host={os.environ.get('PG_HOST', 'localhost')} port={os.environ.get('PG_PORT', '5432')} "
        f"dbname={os.environ.get('PG_DB', 'shop')} user={os.environ.get('PG_USER', 'postgres')} "
        f"password={os.environ.get('PG_PASSWORD', 'postgres')}"
    )
    for attempt in range(30):
        try:
            return psycopg.connect(dsn, autocommit=False)
        except psycopg.OperationalError:
            print(f"waiting for postgres (attempt {attempt + 1})")
            time.sleep(2)
    raise SystemExit("could not connect to postgres")


def new_customer(cur, rng: random.Random) -> None:
    first, last = rng.choice(FIRST), rng.choice(LAST)
    email = f"{first}.{last}{rng.randint(1, 9999)}@example.com".lower()
    cur.execute(
        "INSERT INTO customers (full_name, email, state) VALUES (%s, %s, %s)",
        (f"{first} {last}", email, rng.choice(STATES)),
    )


def seed(conn, rng: random.Random) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM products")
        if cur.fetchone()[0] == 0:
            for i in range(60):
                category = rng.choice(list(CATEGORIES))
                lo, hi = CATEGORIES[category]
                cur.execute(
                    "INSERT INTO products (name, category, unit_price) VALUES (%s, %s, %s)",
                    (f"{category.title()} item {i + 1:03d}", category, round(rng.uniform(lo, hi), 2)),
                )
        cur.execute("SELECT count(*) FROM customers")
        if cur.fetchone()[0] == 0:
            for _ in range(250):
                new_customer(cur, rng)
    conn.commit()
    print("seed complete")


def new_order(cur, rng: random.Random) -> None:
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    customer_id = cur.fetchone()[0]
    cur.execute("SELECT product_id, unit_price FROM products ORDER BY random() LIMIT %s",
                (rng.randint(1, 4),))
    items = [(pid, rng.randint(1, 3), price) for pid, price in cur.fetchall()]
    total = order_total([(q, p) for _, q, p in items])
    cur.execute(
        "INSERT INTO orders (customer_id, status, total_amount) VALUES (%s, 'placed', %s) RETURNING order_id",
        (customer_id, total),
    )
    order_id = cur.fetchone()[0]
    cur.executemany(
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
        [(order_id, pid, q, p) for pid, q, p in items],
    )


def advance_order(cur, rng: random.Random) -> None:
    cur.execute(
        "SELECT order_id, status FROM orders WHERE status IN ('placed','paid','shipped') "
        "ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return
    order_id, status = row
    nxt = next_status(status, rng)
    if nxt:
        cur.execute("UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
                    (nxt, order_id))


def update_customer(cur, rng: random.Random) -> None:
    cur.execute("SELECT customer_id, full_name FROM customers ORDER BY random() LIMIT 1")
    customer_id, name = cur.fetchone()
    if rng.random() < 0.5:
        email = f"{name.replace(' ', '.').lower()}{rng.randint(1, 9999)}@example.com"
        cur.execute("UPDATE customers SET email = %s, updated_at = now() WHERE customer_id = %s",
                    (email, customer_id))
    else:  # moved to a new state: a classic slowly changing dimension
        cur.execute("UPDATE customers SET state = %s, updated_at = now() WHERE customer_id = %s",
                    (rng.choice(STATES), customer_id))


def reprice_product(cur, rng: random.Random) -> None:
    factor = Decimal(str(round(rng.uniform(0.85, 1.15), 2)))
    cur.execute(
        "UPDATE products SET unit_price = round(unit_price * %s, 2), updated_at = now() "
        "WHERE product_id = (SELECT product_id FROM products ORDER BY random() LIMIT 1)",
        (factor,),
    )


def purge_cancelled(cur, rng: random.Random) -> None:
    # Hard delete (cascades to order_items) to exercise CDC delete handling.
    cur.execute(
        "DELETE FROM orders WHERE order_id = (SELECT order_id FROM orders "
        "WHERE status = 'cancelled' ORDER BY updated_at LIMIT 1)"
    )


HANDLERS = {
    "new_order": new_order,
    "advance_order": advance_order,
    "update_customer": update_customer,
    "new_customer": new_customer,
    "reprice_product": reprice_product,
    "purge_cancelled": purge_cancelled,
}


def main() -> None:
    rate = float(os.environ.get("EVENTS_PER_SEC", "10"))
    max_events = int(os.environ["MAX_EVENTS"]) if os.environ.get("MAX_EVENTS") else None
    rng = random.Random(int(os.environ["SEED"])) if os.environ.get("SEED") else random.Random()

    conn = connect()
    seed(conn, rng)
    done = 0
    while max_events is None or done < max_events:
        action = pick_action(rng)
        try:
            with conn.cursor() as cur:
                HANDLERS[action](cur, rng)
            conn.commit()  # each action is one transaction
        except psycopg.Error as e:
            conn.rollback()
            print(f"{action} failed: {e}")
        done += 1
        if done % 500 == 0:
            print(f"{done} actions applied")
        time.sleep(1 / rate)
    print(f"finished after {done} actions")


if __name__ == "__main__":
    main()
