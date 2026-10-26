"""Pure transformation logic for Debezium change events.

Everything here is plain DataFrame / string logic with no Kafka or Iceberg
dependency, so it is unit-tested with a local SparkSession (see tests/).

Debezium event value (JSON converter, schemas disabled):
    {"before": {...} | null, "after": {...} | null,
     "source": {"lsn": 123, "ts_ms": 1700000000000, "snapshot": "false", ...},
     "op": "c" | "u" | "d" | "r", "ts_ms": ...}
    c = insert, u = update, d = delete, r = read (initial snapshot)
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.tables import META_COLUMNS, TOPIC_PREFIX, Table


def parse_envelope(raw: DataFrame) -> DataFrame:
    """Kafka rows (topic, partition, offset, timestamp, key, value as strings)
    -> one row per change event with the envelope fields pulled out."""
    v = F.col("value")
    return raw.select(
        F.regexp_replace("topic", f"^{TOPIC_PREFIX.replace('.', '[.]')}", "").alias("table_name"),
        F.get_json_object(v, "$.op").alias("op"),
        F.get_json_object(v, "$.source.lsn").cast("bigint").alias("lsn"),
        (F.get_json_object(v, "$.source.ts_ms").cast("bigint") / 1000)
        .cast("timestamp")
        .alias("source_ts"),
        F.get_json_object(v, "$.before").alias("before_json"),
        F.get_json_object(v, "$.after").alias("after_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").alias("event_key"),
        F.col("value").alias("payload"),
    ).where(F.col("op").isNotNull())  # drops tombstones / non-Debezium messages


def changes_for_table(events: DataFrame, table: Table) -> DataFrame:
    """Typed rows for one table. Deletes take their values from `before`
    (the row being removed); everything else from `after`."""
    string_schema = StructType([StructField(c, StringType()) for c in table.column_names])
    row_json = F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json"))

    parsed = events.where(F.col("table_name") == table.name).select(
        F.from_json(row_json, string_schema).alias("r"),
        F.col("op").alias("_op"),
        F.col("lsn").alias("_lsn"),
        F.col("source_ts").alias("_source_ts"),
        F.col("kafka_partition").alias("_kafka_partition"),
        F.col("kafka_offset").alias("_kafka_offset"),
    )
    typed_cols = [F.col(f"r.{c}").cast(t).alias(c) for c, t in table.columns]
    return parsed.select(
        *typed_cols, "_op", "_lsn", "_source_ts", "_kafka_partition", "_kafka_offset"
    ).where(F.col(table.pk).isNotNull())


def latest_per_key(changes: DataFrame, pk: str) -> DataFrame:
    """Keep only the newest change per primary key within a micro-batch.

    MERGE requires at most one source row per target row, and only the final
    state matters. Debezium keys messages by primary key, so all changes for
    one row land in the same Kafka partition in commit order; LSN orders them
    and the Kafka offset breaks ties.
    """
    w = Window.partitionBy(pk).orderBy(F.col("_lsn").desc_nulls_last(), F.col("_kafka_offset").desc())
    return (
        changes.withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn", "_kafka_partition", "_kafka_offset")
    )


def silver_ddl(table: Table) -> str:
    cols = ",\n  ".join(f"{c} {t}" for c, t in table.columns + META_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {table.silver} (\n  {cols}\n) USING iceberg\n"
        "TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read', "
        "'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read')"
    )


def build_merge_sql(table: Table, source_view: str) -> str:
    """Idempotent upsert/delete of the latest changes into the silver table.

    The `s._lsn >= t._lsn` guard means replaying an old batch (for example
    after a crash before the checkpoint was written) can never overwrite a
    newer row with an older version.
    """
    pk = table.pk
    all_cols = table.column_names + ["_lsn", "_source_ts", "_ingested_at"]
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in all_cols if c != pk)
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"s.{c}" for c in all_cols)
    return (
        f"MERGE INTO {table.silver} t\n"
        f"USING {source_view} s\n"
        f"ON t.{pk} = s.{pk}\n"
        "WHEN MATCHED AND s._op = 'd' AND s._lsn >= t._lsn THEN DELETE\n"
        f"WHEN MATCHED AND s._op <> 'd' AND s._lsn >= t._lsn THEN UPDATE SET {set_clause}\n"
        f"WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
