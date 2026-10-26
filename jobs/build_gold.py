"""Batch job: build business-facing gold tables from silver and bronze.

  gold.daily_sales          - orders, cancellations and revenue per day
  gold.product_performance  - units and revenue per product / category
  gold.customer_ltv         - lifetime value per customer
  gold.dim_customer_history - SCD Type 2 customer dimension, rebuilt from the
                              bronze change log (CDC gives you history for free)

Each table is fully rebuilt (CREATE OR REPLACE), which is simple and
idempotent at this data size; Iceberg keeps prior snapshots for time travel.
"""

from pipeline.spark_session import build_spark
from pipeline.tables import CATALOG

S = f"{CATALOG}.silver"
G = f"{CATALOG}.gold"
BRONZE = f"{CATALOG}.bronze.cdc_events"

GOLD_SQL = {
    "daily_sales": f"""
        SELECT
          to_date(o.created_at)                                        AS order_date,
          count(*)                                                     AS orders,
          sum(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END)      AS cancelled_orders,
          round(avg(CASE WHEN o.status = 'cancelled' THEN 1.0 ELSE 0 END), 4) AS cancel_rate,
          sum(CASE WHEN o.status <> 'cancelled' THEN o.total_amount ELSE 0 END) AS gross_revenue,
          round(avg(CASE WHEN o.status <> 'cancelled' THEN o.total_amount END), 2) AS avg_order_value,
          sum(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END)      AS delivered_orders
        FROM {S}.orders o
        GROUP BY to_date(o.created_at)
    """,
    "product_performance": f"""
        SELECT
          p.product_id, p.name, p.category, p.unit_price AS current_price,
          coalesce(sum(i.quantity), 0)                    AS units_sold,
          coalesce(sum(i.quantity * i.unit_price), 0)     AS revenue,
          count(DISTINCT i.order_id)                      AS orders
        FROM {S}.products p
        LEFT JOIN (
          SELECT i.* FROM {S}.order_items i
          JOIN {S}.orders o ON o.order_id = i.order_id
          WHERE o.status <> 'cancelled'
        ) i ON i.product_id = p.product_id
        GROUP BY p.product_id, p.name, p.category, p.unit_price
    """,
    "customer_ltv": f"""
        SELECT
          c.customer_id, c.full_name, c.state,
          count(o.order_id)                                                    AS orders,
          coalesce(sum(CASE WHEN o.status <> 'cancelled' THEN o.total_amount END), 0) AS lifetime_value,
          min(o.created_at)                                                    AS first_order_at,
          max(o.created_at)                                                    AS last_order_at
        FROM {S}.customers c
        LEFT JOIN {S}.orders o ON o.customer_id = c.customer_id
        GROUP BY c.customer_id, c.full_name, c.state
    """,
    # SCD Type 2: one row per version of a customer's tracked attributes
    # (email, state). Versions come from the bronze change log ordered by LSN.
    "dim_customer_history": f"""
        WITH changes AS (
          SELECT
            CAST(get_json_object(payload, '$.after.customer_id') AS BIGINT)   AS customer_id,
            get_json_object(payload, '$.after.full_name')                     AS full_name,
            get_json_object(payload, '$.after.email')                         AS email,
            get_json_object(payload, '$.after.state')                         AS state,
            CAST(get_json_object(payload, '$.after.created_at') AS TIMESTAMP) AS created_at,
            source_ts, lsn, kafka_offset
          FROM {BRONZE}
          WHERE table_name = 'customers' AND op IN ('r', 'c', 'u')
        ),
        deduped AS (   -- bronze is append-only; collapse exact replays
          SELECT DISTINCT * FROM changes
        ),
        flagged AS (   -- keep only changes to tracked attributes
          SELECT *,
            NOT (lag(email) OVER w <=> email)
              OR NOT (lag(state) OVER w <=> state) AS is_new_version
          FROM deduped
          WINDOW w AS (PARTITION BY customer_id ORDER BY lsn, kafka_offset)
        ),
        versions AS (
          SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY lsn, kafka_offset) AS version
          FROM flagged WHERE is_new_version
        )
        SELECT
          customer_id, version, full_name, email, state,
          CASE WHEN version = 1 THEN least(created_at, source_ts) ELSE source_ts END AS valid_from,
          lead(source_ts) OVER (PARTITION BY customer_id ORDER BY version) AS valid_to,
          lead(source_ts) OVER (PARTITION BY customer_id ORDER BY version) IS NULL AS is_current
        FROM versions
    """,
}


def main() -> None:
    spark = build_spark("build-gold")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {G}")
    for name, query in GOLD_SQL.items():
        spark.sql(f"CREATE OR REPLACE TABLE {G}.{name} USING iceberg AS {query}")
        rows = spark.table(f"{G}.{name}").count()
        print(f"built {G}.{name}: {rows} rows")

    print("\nDaily sales (latest 7 days):")
    spark.sql(f"SELECT * FROM {G}.daily_sales ORDER BY order_date DESC LIMIT 7").show(truncate=False)
    print("Customers whose state changed (SCD2 history):")
    spark.sql(f"""
        SELECT customer_id, version, state, email, valid_from, valid_to, is_current
        FROM {G}.dim_customer_history
        WHERE customer_id IN (SELECT customer_id FROM {G}.dim_customer_history WHERE version > 1 LIMIT 3)
        ORDER BY customer_id, version
    """).show(truncate=False)


if __name__ == "__main__":
    main()
