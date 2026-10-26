from datetime import datetime
from decimal import Decimal

from pipeline.cdc import build_merge_sql, changes_for_table, latest_per_key, parse_envelope, silver_ddl
from pipeline.tables import TABLES, table_for_topic

ORDERS = TABLES["orders"]


def order(order_id, status, total="10.00"):
    return {"order_id": order_id, "customer_id": 7, "status": status, "total_amount": total,
            "created_at": "2026-10-20T22:31:05.123456Z", "updated_at": "2026-10-20T22:35:00Z"}


def test_parse_envelope_extracts_fields(kafka_rows):
    df = parse_envelope(kafka_rows([("orders", "c", None, order(1, "placed"), 100, 0)]))
    row = df.first()
    assert row.table_name == "orders"
    assert row.op == "c"
    assert row.lsn == 100
    assert row.before_json is None
    assert row.source_ts == datetime(2025, 10, 9, 8, 53, 20)  # ts_ms 1_760_000_000_000 in UTC


def test_parse_envelope_drops_tombstones(spark):
    raw = spark.createDataFrame(
        [("shop.public.orders", 0, 5, None, '{"order_id": 1}', None)],
        "topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP, key STRING, value STRING",
    )
    assert parse_envelope(raw).count() == 0


def test_changes_are_typed(kafka_rows):
    events = parse_envelope(kafka_rows([("orders", "c", None, order(1, "placed", "123.45"), 100, 0)]))
    row = changes_for_table(events, ORDERS).first()
    assert row.order_id == 1
    assert row.total_amount == Decimal("123.45")  # Debezium sends decimals as strings
    assert row.created_at == datetime(2026, 10, 20, 22, 31, 5, 123456)  # ISO-8601 with Z parsed
    assert row._op == "c"


def test_delete_uses_before_image(kafka_rows):
    events = parse_envelope(kafka_rows([("orders", "d", order(9, "cancelled"), None, 300, 4)]))
    row = changes_for_table(events, ORDERS).first()
    assert row.order_id == 9
    assert row.status == "cancelled"
    assert row._op == "d"


def test_changes_for_table_filters_other_tables(kafka_rows):
    events = parse_envelope(kafka_rows([
        ("orders", "c", None, order(1, "placed"), 100, 0),
        ("customers", "c", None, {"customer_id": 7, "full_name": "A B", "email": "a@b.c",
                                  "state": "IN", "created_at": "2026-10-20T00:00:00Z",
                                  "updated_at": "2026-10-20T00:00:00Z"}, 101, 0),
    ]))
    assert changes_for_table(events, ORDERS).count() == 1
    assert changes_for_table(events, TABLES["customers"]).count() == 1


def test_latest_per_key_keeps_newest_change(kafka_rows):
    events = parse_envelope(kafka_rows([
        ("orders", "c", None, order(1, "placed"), 100, 0),
        ("orders", "u", order(1, "placed"), order(1, "paid"), 200, 1),
        ("orders", "u", order(1, "paid"), order(1, "shipped"), 300, 2),
        ("orders", "c", None, order(2, "placed"), 150, 3),
    ]))
    latest = {r.order_id: r for r in latest_per_key(changes_for_table(events, ORDERS), "order_id").collect()}
    assert set(latest) == {1, 2}
    assert latest[1].status == "shipped" and latest[1]._lsn == 300
    assert latest[2].status == "placed"


def test_latest_per_key_insert_then_delete_resolves_to_delete(kafka_rows):
    events = parse_envelope(kafka_rows([
        ("orders", "c", None, order(5, "placed"), 100, 0),
        ("orders", "d", order(5, "cancelled"), None, 400, 1),
    ]))
    rows = latest_per_key(changes_for_table(events, ORDERS), "order_id").collect()
    assert len(rows) == 1 and rows[0]._op == "d"


def test_offset_breaks_lsn_ties(kafka_rows):
    events = parse_envelope(kafka_rows([
        ("orders", "u", None, order(1, "paid"), 500, 10),
        ("orders", "u", None, order(1, "shipped"), 500, 11),
    ]))
    row = latest_per_key(changes_for_table(events, ORDERS), "order_id").first()
    assert row.status == "shipped"


def test_merge_sql_is_lsn_guarded_and_handles_deletes():
    sql = build_merge_sql(ORDERS, "changes_orders")
    assert sql.startswith("MERGE INTO lake.silver.orders t")
    assert "ON t.order_id = s.order_id" in sql
    assert "s._op = 'd' AND s._lsn >= t._lsn THEN DELETE" in sql
    assert "s._lsn >= t._lsn THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT" in sql
    assert "t.order_id = s.order_id," not in sql  # never update the key


def test_silver_ddl_contains_columns_and_metadata():
    ddl = silver_ddl(ORDERS)
    for col, typ in ORDERS.columns:
        assert f"{col} {typ}" in ddl
    assert "_lsn BIGINT" in ddl and "merge-on-read" in ddl


def test_table_for_topic():
    assert table_for_topic("shop.public.orders").name == "orders"
    assert table_for_topic("__debezium-heartbeat.shop") is None
    assert table_for_topic("shop.public.unknown") is None
