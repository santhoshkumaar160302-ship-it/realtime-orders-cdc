from decimal import Decimal

from jobs.reconcile import compare, is_clean
from pipeline.tables import TABLES

PRODUCTS = TABLES["products"]
SCHEMA = "product_id BIGINT, name STRING, category STRING, unit_price DECIMAL(10,2), updated_at TIMESTAMP"


def test_reconcile_classifies_every_kind_of_discrepancy(spark):
    src = spark.createDataFrame([
        (1, "Lamp", "home", Decimal("10.00"), None),
        (2, "Book", "books", Decimal("5.00"), None),   # price differs in lake
        (3, "Ball", "toys", Decimal("7.50"), None),    # never arrived in lake
    ], SCHEMA)
    lake = spark.createDataFrame([
        (1, "Lamp", "home", Decimal("10.00"), None),
        (2, "Book", "books", Decimal("6.00"), None),
        (4, "Gone", "toys", Decimal("1.00"), None),    # deleted in source, still in lake
    ], SCHEMA)

    r = compare(src, lake, PRODUCTS)
    assert r["source_rows"] == 3
    assert r["matched"] == 1
    assert r["value_mismatch"] == 1 and r["sample_keys"]["value_mismatch"] == [2]
    assert r["missing_in_lake"] == 1 and r["sample_keys"]["missing_in_lake"] == [3]
    assert r["extra_in_lake"] == 1 and r["sample_keys"]["extra_in_lake"] == [4]
    assert not is_clean([r])


def test_reconcile_treats_nulls_consistently(spark):
    rows = [(1, None, "home", Decimal("1.00"), None)]
    r = compare(spark.createDataFrame(rows, SCHEMA), spark.createDataFrame(rows, SCHEMA), PRODUCTS)
    assert r["matched"] == 1 and is_clean([r])
