"""Print the numbers you can quote on your resume and in interviews:
throughput, end-to-end latency, row counts, plus an Iceberg time-travel demo.
"""

from pipeline.spark_session import build_spark
from pipeline.tables import CATALOG, TABLES


def main() -> None:
    spark = build_spark("pipeline-stats")
    m = f"{CATALOG}.ops.batch_metrics"
    spark.sql(f"REFRESH TABLE {m}")

    print("== Streaming performance (excluding the initial snapshot batch) ==")
    spark.sql(f"""
        SELECT
          count(*)                                     AS micro_batches,
          sum(events)                                  AS change_events,
          sum(inserts) AS inserts, sum(updates) AS updates, sum(deletes) AS deletes,
          round(sum(events) / sum(duration_seconds), 1) AS events_per_sec_processing,
          round(percentile_approx(lag_seconds, 0.5), 1)  AS p50_lag_s,
          round(percentile_approx(lag_seconds, 0.95), 1) AS p95_lag_s,
          round(max(lag_seconds), 1)                     AS max_lag_s
        FROM {m}
        WHERE snapshot_reads = 0
    """).show(truncate=False)

    print("== Rows in the lake ==")
    for t in TABLES.values():
        print(f"{t.silver:<28}{spark.table(t.silver).count():>10}")
    print(f"{CATALOG + '.bronze.cdc_events':<28}{spark.table(f'{CATALOG}.bronze.cdc_events').count():>10}")

    print("\n== Iceberg time travel: lake.silver.orders by status, first snapshot vs now ==")
    first = spark.sql(
        f"SELECT snapshot_id FROM {CATALOG}.silver.orders.snapshots ORDER BY committed_at LIMIT 1"
    ).first()
    if first:
        spark.sql(
            f"SELECT status, count(*) AS orders_then FROM {CATALOG}.silver.orders "
            f"VERSION AS OF {first[0]} GROUP BY status ORDER BY status"
        ).show()
    spark.sql(
        f"SELECT status, count(*) AS orders_now FROM {CATALOG}.silver.orders GROUP BY status ORDER BY status"
    ).show()


if __name__ == "__main__":
    main()
