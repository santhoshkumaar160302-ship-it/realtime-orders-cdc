"""Data quality checks on the silver layer.

Each check is a SQL query that returns the *offending* rows; a check passes
when it returns zero rows. `error` checks fail the job (exit 1), `warn`
checks are reported only. Results go to <warehouse>/_reports/.
"""

import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipeline.spark_session import build_spark, warehouse_path
from pipeline.tables import TABLES

FRESHNESS_MINUTES = int(os.environ.get("FRESHNESS_MINUTES", "15"))


@dataclass(frozen=True)
class Check:
    name: str
    severity: str  # "error" | "warn"
    sql: str


def build_checks() -> list[Check]:
    checks: list[Check] = []
    for t in TABLES.values():
        checks.append(Check(
            f"{t.name}.{t.pk}_unique", "error",
            f"SELECT {t.pk} FROM {t.silver} GROUP BY {t.pk} HAVING count(*) > 1",
        ))
        checks.append(Check(
            f"{t.name}.no_null_columns", "error",
            f"SELECT {t.pk} FROM {t.silver} WHERE "
            + " OR ".join(f"{c} IS NULL" for c in t.column_names),
        ))

    s = {name: t.silver for name, t in TABLES.items()}
    checks += [
        Check("orders.valid_status", "error",
              f"SELECT order_id FROM {s['orders']} "
              "WHERE status NOT IN ('placed','paid','shipped','delivered','cancelled')"),
        Check("order_items.positive_quantity", "error",
              f"SELECT order_item_id FROM {s['order_items']} WHERE quantity <= 0"),
        Check("products.non_negative_price", "error",
              f"SELECT product_id FROM {s['products']} WHERE unit_price < 0"),
        Check("orders.customer_exists", "error",
              f"SELECT o.order_id FROM {s['orders']} o LEFT ANTI JOIN {s['customers']} c "
              "ON o.customer_id = c.customer_id"),
        Check("order_items.order_exists", "error",
              f"SELECT i.order_item_id FROM {s['order_items']} i LEFT ANTI JOIN {s['orders']} o "
              "ON i.order_id = o.order_id"),
        # Cross-table consistency: an order's total must equal the sum of its
        # line items. Catches a transaction that was only partly applied.
        Check("orders.total_matches_items", "error",
              f"SELECT o.order_id FROM {s['orders']} o JOIN ("
              f"  SELECT order_id, sum(quantity * unit_price) AS items_total "
              f"  FROM {s['order_items']} GROUP BY order_id) i ON o.order_id = i.order_id "
              "WHERE abs(o.total_amount - i.items_total) > 0.01"),
        Check("orders.freshness", "warn",
              f"SELECT max(_source_ts) AS last_change FROM {s['orders']} "
              f"HAVING max(_source_ts) < current_timestamp() - INTERVAL {FRESHNESS_MINUTES} MINUTES"),
    ]
    return checks


def main() -> None:
    spark = build_spark("quality-checks")
    for t in TABLES.values():
        spark.sql(f"REFRESH TABLE {t.silver}")

    results = []
    for check in build_checks():
        df = spark.sql(check.sql)
        failing = df.count()
        sample = [list(r) for r in df.limit(5).collect()] if failing else []
        results.append({"check": check.name, "severity": check.severity,
                        "failing_rows": failing, "sample": sample,
                        "passed": failing == 0})
        mark = "PASS" if failing == 0 else ("FAIL" if check.severity == "error" else "WARN")
        print(f"[{mark}] {check.name}: {failing} failing rows")

    errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    report = {"run_at": datetime.now(UTC).isoformat(),
              "status": "FAIL" if errors else "PASS", "checks": results}
    out_dir = Path(warehouse_path()) / "_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"quality_{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nQUALITY {report['status']}: {len(errors)} error check(s) failed  (report: {out})")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
