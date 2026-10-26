"""Shared SparkSession factory configured for Iceberg, Kafka and Postgres JDBC."""

import os

from pyspark.sql import SparkSession

SPARK_VERSION = "3.5.3"
ICEBERG_VERSION = "1.6.1"

PACKAGES = ",".join(
    [
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
        "org.postgresql:postgresql:42.7.4",
    ]
)


def warehouse_path() -> str:
    return os.environ.get("WAREHOUSE", "/warehouse")


def build_spark(app_name: str, shuffle_partitions: int = 4) -> SparkSession:
    wh = warehouse_path()
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages", PACKAGES)
        .config("spark.jars.ivy", os.environ.get("IVY_HOME", "/root/.ivy2"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"file://{wh}/iceberg")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def pg_jdbc_url() -> str:
    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DB", "shop")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def pg_props() -> dict[str, str]:
    return {
        "user": os.environ.get("PG_USER", "postgres"),
        "password": os.environ.get("PG_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver",
    }
