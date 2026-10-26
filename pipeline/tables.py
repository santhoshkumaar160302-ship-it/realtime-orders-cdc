"""Single source of truth for the tables the pipeline replicates.

Every job (streaming merge, reconciliation, quality checks) reads its
column list and primary key from here, so adding a table means editing
one place.
"""

from dataclasses import dataclass

CATALOG = "lake"
TOPIC_PREFIX = "shop.public."


@dataclass(frozen=True)
class Table:
    name: str
    pk: str
    columns: tuple[tuple[str, str], ...]  # (column, Spark SQL type)

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]

    @property
    def topic(self) -> str:
        return f"{TOPIC_PREFIX}{self.name}"

    @property
    def silver(self) -> str:
        return f"{CATALOG}.silver.{self.name}"


TABLES: dict[str, Table] = {
    t.name: t
    for t in [
        Table(
            "customers",
            "customer_id",
            (
                ("customer_id", "BIGINT"),
                ("full_name", "STRING"),
                ("email", "STRING"),
                ("state", "STRING"),
                ("created_at", "TIMESTAMP"),
                ("updated_at", "TIMESTAMP"),
            ),
        ),
        Table(
            "products",
            "product_id",
            (
                ("product_id", "BIGINT"),
                ("name", "STRING"),
                ("category", "STRING"),
                ("unit_price", "DECIMAL(10,2)"),
                ("updated_at", "TIMESTAMP"),
            ),
        ),
        Table(
            "orders",
            "order_id",
            (
                ("order_id", "BIGINT"),
                ("customer_id", "BIGINT"),
                ("status", "STRING"),
                ("total_amount", "DECIMAL(12,2)"),
                ("created_at", "TIMESTAMP"),
                ("updated_at", "TIMESTAMP"),
            ),
        ),
        Table(
            "order_items",
            "order_item_id",
            (
                ("order_item_id", "BIGINT"),
                ("order_id", "BIGINT"),
                ("product_id", "BIGINT"),
                ("quantity", "INT"),
                ("unit_price", "DECIMAL(10,2)"),
            ),
        ),
    ]
}

# Metadata columns added to every silver table.
META_COLUMNS: tuple[tuple[str, str], ...] = (
    ("_lsn", "BIGINT"),            # Postgres log sequence number of the last applied change
    ("_source_ts", "TIMESTAMP"),   # when the change was committed in Postgres
    ("_ingested_at", "TIMESTAMP"), # when the lake applied it
)


def table_for_topic(topic: str) -> Table | None:
    if not topic.startswith(TOPIC_PREFIX):
        return None
    return TABLES.get(topic[len(TOPIC_PREFIX):])
