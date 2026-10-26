"""Run the gold and quality-check SQL against small in-memory fixtures.

Iceberg isn't needed to validate the SQL itself, so catalog table names are
rewritten to temp views (lake.silver.orders -> silver_orders).
"""

import re
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from jobs.build_gold import GOLD_SQL
from jobs.quality_checks import build_checks
from tests.conftest import debezium_event


def local(sql: str) -> str:
    return re.sub(r"\blake\.(silver|bronze|gold)\.(\w+)", r"\1_\2", sql)


T0 = datetime(2026, 10, 1, 12, 0, 0)


@pytest.fixture
def silver(spark):
    spark.createDataFrame([
        (1, "Ava Smith", "ava@example.com", "IN", T0, T0),
        (2, "Liam Lee", "liam@example.com", "CA", T0, T0),
    ], "customer_id BIGINT, full_name STRING, email STRING, state STRING, created_at TIMESTAMP, "
       "updated_at TIMESTAMP").createOrReplaceTempView("silver_customers")
    spark.createDataFrame([
        (10, "Lamp", "home", Decimal("20.00"), T0),
        (11, "Book", "books", Decimal("5.00"), T0),
    ], "product_id BIGINT, name STRING, category STRING, unit_price DECIMAL(10,2), updated_at TIMESTAMP"
    ).createOrReplaceTempView("silver_products")
    spark.createDataFrame([
        (100, 1, "delivered", Decimal("45.00"), T0, T0, T0),
        (101, 1, "cancelled", Decimal("5.00"), T0, T0, T0),
        (102, 2, "paid", Decimal("20.00"), T0, T0, T0),
    ], "order_id BIGINT, customer_id BIGINT, status STRING, total_amount DECIMAL(12,2), "
       "created_at TIMESTAMP, updated_at TIMESTAMP, _source_ts TIMESTAMP").createOrReplaceTempView("silver_orders")
    spark.createDataFrame([
        (1000, 100, 10, 2, Decimal("20.00")),
        (1001, 100, 11, 1, Decimal("5.00")),
        (1002, 101, 11, 1, Decimal("5.00")),
        (1003, 102, 10, 1, Decimal("20.00")),
    ], "order_item_id BIGINT, order_id BIGINT, product_id BIGINT, quantity INT, unit_price DECIMAL(10,2)"
    ).createOrReplaceTempView("silver_order_items")


@pytest.fixture
def bronze(spark):
    def cust(state, email):
        return {"customer_id": 1, "full_name": "Ava Smith", "email": email, "state": state,
                "created_at": "2026-09-01T00:00:00Z", "updated_at": "2026-10-01T00:00:00Z"}
    rows = []
    history = [  # (op, state, email, lsn, ts_ms)
        ("r", "IN", "ava@example.com", 10, 1_790_000_000_000),
        ("u", "IN", "ava@example.com", 20, 1_790_100_000_000),  # no tracked change -> no new version
        ("u", "CA", "ava@example.com", 30, 1_790_200_000_000),  # moved -> version 2
        ("u", "CA", "ava2@example.com", 40, 1_790_300_000_000), # new email -> version 3
    ]
    for i, (op, state, email, lsn, ts) in enumerate(history):
        payload = debezium_event("customers", op, None, cust(state, email), lsn, ts)
        source_ts = datetime.fromtimestamp(ts / 1000, UTC).replace(tzinfo=None)
        rows.append(("customers", op, lsn, source_ts, i, payload))
    spark.createDataFrame(
        rows, "table_name STRING, op STRING, lsn BIGINT, source_ts TIMESTAMP, kafka_offset BIGINT, payload STRING"
    ).createOrReplaceTempView("bronze_cdc_events")


def test_daily_sales(spark, silver):
    row = spark.sql(local(GOLD_SQL["daily_sales"])).first()
    assert row.orders == 3 and row.cancelled_orders == 1
    assert row.gross_revenue == Decimal("65.00")  # cancelled order excluded
    assert row.delivered_orders == 1


def test_product_performance_excludes_cancelled_but_keeps_products(spark, silver):
    rows = {r.product_id: r for r in spark.sql(local(GOLD_SQL["product_performance"])).collect()}
    assert rows[10].units_sold == 3 and rows[10].revenue == Decimal("60.00")
    assert rows[11].units_sold == 1  # the cancelled order's book is not counted


def test_customer_ltv(spark, silver):
    rows = {r.customer_id: r for r in spark.sql(local(GOLD_SQL["customer_ltv"])).collect()}
    assert rows[1].orders == 2 and rows[1].lifetime_value == Decimal("45.00")
    assert rows[2].lifetime_value == Decimal("20.00")


def test_scd2_customer_history(spark, bronze):
    rows = spark.sql(local(GOLD_SQL["dim_customer_history"])).orderBy("version").collect()
    assert [(r.version, r.state, r.email) for r in rows] == [
        (1, "IN", "ava@example.com"), (2, "CA", "ava@example.com"), (3, "CA", "ava2@example.com"),
    ]
    assert rows[0].valid_from == datetime(2026, 9, 1)          # first version starts at creation
    assert rows[0].valid_to == rows[1].valid_from               # no gaps between versions
    assert [r.is_current for r in rows] == [False, False, True]


def test_quality_checks_pass_on_clean_data(spark, silver):
    for check in build_checks():
        if check.severity == "error":
            assert spark.sql(local(check.sql)).count() == 0, check.name


def test_quality_checks_catch_bad_rows(spark, silver):
    bad = spark.createDataFrame(
        [(103, 99, "lost", Decimal("1.00"), T0, T0, T0)], spark.table("silver_orders").schema
    )
    spark.table("silver_orders").unionByName(bad).createOrReplaceTempView("silver_orders")
    failing = {c.name for c in build_checks() if spark.sql(local(c.sql)).count() > 0}
    assert {"orders.valid_status", "orders.customer_exists"} <= failing
