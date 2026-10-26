"""Iceberg table maintenance.

Streaming MERGEs in merge-on-read mode write many small data files and
delete files. Left alone, reads get slower every batch. This job:
  1. compacts small data files (rewrite_data_files)
  2. compacts position-delete files (rewrite_position_delete_files)
  3. expires old snapshots so metadata and storage don't grow forever

In production this runs on a schedule (Airflow, cron). Running it while the
streamer is writing is allowed; Iceberg's optimistic concurrency retries or
fails the compaction commit cleanly without corrupting the table.
"""

import os

from pipeline.spark_session import build_spark
from pipeline.tables import CATALOG, TABLES

RETAIN_HOURS = int(os.environ.get("SNAPSHOT_RETAIN_HOURS", "24"))


def file_stats(spark, table: str) -> tuple[int, int]:
    data = spark.sql(f"SELECT count(*) FROM {table}.files WHERE content = 0").first()[0]
    deletes = spark.sql(f"SELECT count(*) FROM {table}.files WHERE content <> 0").first()[0]
    return data, deletes


def main() -> None:
    spark = build_spark("maintenance")
    for t in TABLES.values():
        ident = f"silver.{t.name}"
        before = file_stats(spark, t.silver)
        try:
            spark.sql(
                f"CALL {CATALOG}.system.rewrite_data_files(table => '{ident}', "
                "options => map('partial-progress.enabled', 'true', 'delete-file-threshold', '1'))"
            )
            spark.sql(f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{ident}')")
            spark.sql(
                f"CALL {CATALOG}.system.expire_snapshots(table => '{ident}', "
                f"older_than => current_timestamp() - INTERVAL {RETAIN_HOURS} HOURS, retain_last => 10)"
            )
        except Exception as e:  # noqa: BLE001 - report and continue with other tables
            print(f"{t.silver}: maintenance skipped ({type(e).__name__}: {str(e)[:200]})")
            continue
        after = file_stats(spark, t.silver)
        print(f"{t.silver}: data files {before[0]} -> {after[0]}, delete files {before[1]} -> {after[1]}")


if __name__ == "__main__":
    main()
