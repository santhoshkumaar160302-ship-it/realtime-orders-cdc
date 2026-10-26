"""Streaming job: Debezium topics in Kafka -> Iceberg bronze + silver.

Per micro-batch:
  1. Append every raw change event to `lake.bronze.cdc_events` (an immutable,
     replayable change log). The append is skipped if this batch was already
     committed, so restarts never duplicate bronze rows.
  2. For each table, keep the newest change per primary key and MERGE it into
     `lake.silver.<table>` (inserts, updates and hard deletes). The merge is
     guarded by the Postgres LSN, so replays are harmless.
  3. Record batch metrics (event counts by operation, end-to-end lag) in
     `lake.ops.batch_metrics`.

Exactly-once *results* come from: Spark checkpoints (Kafka offsets) +
idempotent bronze append + LSN-guarded silver merge.
"""

import hashlib
import os
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.cdc import build_merge_sql, changes_for_table, latest_per_key, parse_envelope, silver_ddl
from pipeline.spark_session import build_spark, warehouse_path
from pipeline.tables import CATALOG, TABLES

QUERY_NAME = "cdc_to_iceberg"
BRONZE = f"{CATALOG}.bronze.cdc_events"
METRICS = f"{CATALOG}.ops.batch_metrics"


def create_tables(spark) -> None:
    for ns in ("bronze", "silver", "ops"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
          table_name STRING, op STRING, lsn BIGINT, source_ts TIMESTAMP,
          kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
          kafka_timestamp TIMESTAMP, event_key STRING, payload STRING,
          ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (table_name, days(ingested_at))
        TBLPROPERTIES ('format-version'='2')
    """)
    for table in TABLES.values():
        spark.sql(silver_ddl(table))
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {METRICS} (
          batch_id BIGINT, processed_at TIMESTAMP, events BIGINT,
          inserts BIGINT, updates BIGINT, deletes BIGINT, snapshot_reads BIGINT,
          max_source_ts TIMESTAMP, lag_seconds DOUBLE, duration_seconds DOUBLE
        ) USING iceberg
    """)


def bronze_already_written(spark, batch_key: str) -> bool:
    return (
        spark.sql(
            f"SELECT 1 FROM {BRONZE}.snapshots WHERE summary['cdc.batch-key'] = '{batch_key}' LIMIT 1"
        ).count()
        > 0
    )


def offset_range_key(events: DataFrame) -> str:
    ranges = (
        events.groupBy("kafka_topic", "kafka_partition")
        .agg(F.min("kafka_offset").alias("lo"), F.max("kafka_offset").alias("hi"))
        .orderBy("kafka_topic", "kafka_partition")
        .collect()
    )
    raw = ";".join(f"{r.kafka_topic}/{r.kafka_partition}:{r.lo}-{r.hi}" for r in ranges)
    return hashlib.sha256(raw.encode()).hexdigest()


def process_batch(batch: DataFrame, batch_id: int) -> None:
    started = time.time()
    spark = batch.sparkSession
    events = parse_envelope(batch).persist()
    try:
        stats = events.agg(
            F.count("*").alias("events"),
            F.sum(F.when(F.col("op") == "c", 1).otherwise(0)).alias("inserts"),
            F.sum(F.when(F.col("op") == "u", 1).otherwise(0)).alias("updates"),
            F.sum(F.when(F.col("op") == "d", 1).otherwise(0)).alias("deletes"),
            F.sum(F.when(F.col("op") == "r", 1).otherwise(0)).alias("snapshot_reads"),
            F.max("source_ts").alias("max_source_ts"),
        ).first()
        if not stats or stats["events"] == 0:
            return

        # 1. Bronze: immutable change log, written at most once per batch.
        # The key is derived from the Kafka offset ranges in the batch, so a
        # replayed batch (same offsets) is recognised even after a restart.
        batch_key = offset_range_key(events)
        if not bronze_already_written(spark, batch_key):
            (
                events.drop("before_json", "after_json")
                .withColumn("ingested_at", F.current_timestamp())
                .select(
                    "table_name", "op", "lsn", "source_ts", "kafka_topic", "kafka_partition",
                    "kafka_offset", "kafka_timestamp", "event_key", "payload", "ingested_at",
                )
                .writeTo(BRONZE)
                .option("snapshot-property.cdc.batch-key", batch_key)
                .append()
            )

        # 2. Silver: current state of each source table.
        for table in TABLES.values():
            latest = latest_per_key(changes_for_table(events, table), table.pk).withColumn(
                "_ingested_at", F.current_timestamp()
            )
            if latest.isEmpty():
                continue
            view = f"changes_{table.name}"
            latest.createOrReplaceTempView(view)
            spark.sql(build_merge_sql(table, view))

        # 3. Metrics for monitoring throughput and freshness.
        processed_at = time.time()
        lag = processed_at - stats["max_source_ts"].timestamp() if stats["max_source_ts"] else None
        spark.createDataFrame(
            [(
                batch_id, int(stats["events"]), int(stats["inserts"]), int(stats["updates"]),
                int(stats["deletes"]), int(stats["snapshot_reads"]), stats["max_source_ts"],
                lag, processed_at - started,
            )],
            "batch_id BIGINT, events BIGINT, inserts BIGINT, updates BIGINT, deletes BIGINT, "
            "snapshot_reads BIGINT, max_source_ts TIMESTAMP, lag_seconds DOUBLE, duration_seconds DOUBLE",
        ).withColumn("processed_at", F.current_timestamp()).select(
            "batch_id", "processed_at", "events", "inserts", "updates", "deletes",
            "snapshot_reads", "max_source_ts", "lag_seconds", "duration_seconds",
        ).writeTo(METRICS).append()

        lag_txt = f"{lag:.1f}s" if lag is not None else "n/a"
        print(
            f"batch {batch_id}: {stats['events']} events "
            f"(c={stats['inserts']} u={stats['updates']} d={stats['deletes']} "
            f"r={stats['snapshot_reads']}) lag={lag_txt} took={processed_at - started:.1f}s"
        )
    finally:
        events.unpersist()


def main() -> None:
    spark = build_spark("cdc-to-iceberg")
    create_tables(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
        .option("subscribePattern", r"shop\.public\..*")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000"))
        .option("kafka.metadata.max.age.ms", "10000")  # pick up new topics quickly
        .load()
        .select(
            "topic", "partition", "offset", "timestamp",
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
        )
    )

    query = (
        raw.writeStream.queryName(QUERY_NAME)
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{warehouse_path()}/_checkpoints/{QUERY_NAME}")
        .trigger(processingTime=f"{os.environ.get('TRIGGER_SECONDS', '10')} seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
