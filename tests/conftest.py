import json
import os
import time

import pytest
from pyspark.sql import SparkSession

# Spark returns timestamps in the local timezone; pin it so tests match the containers.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def debezium_event(table, op, before, after, lsn, ts_ms=1_760_000_000_000):
    """Build a Debezium JSON envelope exactly as the connector emits it."""
    return json.dumps({
        "before": before,
        "after": after,
        "source": {"connector": "postgresql", "db": "shop", "schema": "public",
                   "table": table, "lsn": lsn, "ts_ms": ts_ms,
                   "snapshot": "true" if op == "r" else "false"},
        "op": op,
        "ts_ms": ts_ms + 50,
        "transaction": None,
    })


@pytest.fixture
def kafka_rows(spark):
    """Factory: list of (table, op, before, after, lsn, offset) -> raw Kafka DataFrame."""
    def make(events):
        rows = []
        for table, op, before, after, lsn, offset in events:
            pk_src = after or before
            key = json.dumps({k: v for k, v in pk_src.items() if k.endswith("_id")})
            rows.append((f"shop.public.{table}", 0, offset, None, key,
                         debezium_event(table, op, before, after, lsn)))
        return spark.createDataFrame(
            rows, "topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP, key STRING, value STRING"
        )
    return make
