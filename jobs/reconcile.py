"""Row-by-row reconciliation: Postgres (source of truth) vs Iceberg silver.

For every table it hashes each row on both sides, joins on the primary key
and classifies rows as:
  matched        - present on both sides with identical values
  value_mismatch - present on both sides, values differ
  missing_in_lake - in Postgres, not in the lake (dropped or not yet applied)
  extra_in_lake  - in the lake, not in Postgres (a missed delete)

Because the pipeline is continuously streaming, the job retries a few times
to let in-flight changes land before declaring a failure. For a clean result
pause the simulator first (`make pause`).

Writes a JSON report to <warehouse>/_reports/ and exits 1 on any discrepancy,
so it can gate CI or a scheduled run.
"""

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.spark_session import build_spark, pg_jdbc_url, pg_props, warehouse_path
from pipeline.tables import TABLES, Table

ATTEMPTS = int(os.environ.get("RECON_ATTEMPTS", "4"))
WAIT_SECONDS = int(os.environ.get("RECON_WAIT_SECONDS", "15"))
SAMPLE = 5


def row_hash(df: DataFrame, table: Table) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in table.column_names]
    return df.select(F.col(table.pk), F.sha2(F.concat_ws("||", *parts), 256).alias("h"))


def compare(src: DataFrame, lake: DataFrame, table: Table) -> dict:
    pk = table.pk
    joined = row_hash(src, table).alias("s").join(
        row_hash(lake, table).alias("l"), on=pk, how="full_outer"
    )
    classified = joined.select(
        pk,
        F.when(F.col("l.h").isNull(), "missing_in_lake")
        .when(F.col("s.h").isNull(), "extra_in_lake")
        .when(F.col("s.h") != F.col("l.h"), "value_mismatch")
        .otherwise("matched")
        .alias("status"),
    ).cache()

    counts = {r["status"]: r["count"] for r in classified.groupBy("status").count().collect()}
    samples = {}
    for status in ("missing_in_lake", "extra_in_lake", "value_mismatch"):
        if counts.get(status):
            rows = classified.where(F.col("status") == status).orderBy(pk).limit(SAMPLE).collect()
            samples[status] = [r[pk] for r in rows]
    classified.unpersist()

    source_rows = counts.get("matched", 0) + counts.get("missing_in_lake", 0) + counts.get("value_mismatch", 0)
    return {
        "table": table.name,
        "source_rows": source_rows,
        "matched": counts.get("matched", 0),
        "value_mismatch": counts.get("value_mismatch", 0),
        "missing_in_lake": counts.get("missing_in_lake", 0),
        "extra_in_lake": counts.get("extra_in_lake", 0),
        "sample_keys": samples,
    }


def read_source(spark, table: Table) -> DataFrame:
    df = spark.read.jdbc(pg_jdbc_url(), f"public.{table.name}", properties=pg_props())
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in table.columns])


def run_once(spark) -> list[dict]:
    results = []
    for table in TABLES.values():
        spark.sql(f"REFRESH TABLE {table.silver}")
        lake = spark.table(table.silver).select(*table.column_names)
        results.append(compare(read_source(spark, table), lake, table))
    return results


def is_clean(results: list[dict]) -> bool:
    return all(
        r["value_mismatch"] == 0 and r["missing_in_lake"] == 0 and r["extra_in_lake"] == 0
        for r in results
    )


def main() -> None:
    spark = build_spark("reconcile")
    results: list[dict] = []
    for attempt in range(1, ATTEMPTS + 1):
        results = run_once(spark)
        if is_clean(results):
            break
        if attempt < ATTEMPTS:
            print(f"attempt {attempt}: discrepancies found, waiting {WAIT_SECONDS}s for in-flight changes")
            time.sleep(WAIT_SECONDS)

    clean = is_clean(results)
    report = {
        "run_at": datetime.now(UTC).isoformat(),
        "status": "PASS" if clean else "FAIL",
        "attempts": attempt,
        "tables": results,
    }
    out_dir = Path(warehouse_path()) / "_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"reconcile_{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    out.write_text(json.dumps(report, indent=2, default=str))

    print(f"\n{'table':<14}{'source':>10}{'matched':>10}{'mismatch':>10}{'missing':>10}{'extra':>10}")
    for r in results:
        print(f"{r['table']:<14}{r['source_rows']:>10}{r['matched']:>10}{r['value_mismatch']:>10}"
              f"{r['missing_in_lake']:>10}{r['extra_in_lake']:>10}")
    print(f"\nRECONCILIATION {report['status']}  (report: {out})")
    sys.exit(0 if clean else 1)


if __name__ == "__main__":
    main()
