#!/usr/bin/env python3
"""Spark retail DW: ODS CSV -> quality gates -> DWD -> DWS -> ADS (local mode)."""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))
from quality import QualityError, run_ods_gates  # noqa: E402

WAREHOUSE = ROOT / "warehouse"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("spark-retail-dw")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def exec_sql_file(spark: SparkSession, path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    buf: list[str] = []
    for raw in text.splitlines():
        line = raw.split("--", 1)[0].rstrip()
        if line.strip():
            buf.append(line)
    body = "\n".join(buf)
    for stmt in body.split(";"):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)


def load_ods(spark: SparkSession):
    ods = ROOT / "data" / "ods"
    users = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods / "users.csv"))
    )
    orders = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods / "orders.csv"))
        .withColumn("pay_amount", F.col("pay_amount").cast("double"))
    )
    items = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods / "order_items.csv"))
        .withColumn("qty", F.col("qty").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("amount", F.col("amount").cast("double"))
    )
    return users, orders, items


def save_parquet(df, rel: str) -> None:
    """Write a small local table.

    Spark parquet on Windows needs Hadoop winutils + native IO (hadoop.dll).
    This demo is tiny, so Windows (and any Hadoop local-FS failure) falls back
    to a CSV part file via collect() — the same path ADS KPI already uses.
    """
    path = WAREHOUSE / rel
    if os.name != "nt":
        try:
            df.write.mode("overwrite").parquet(str(path))
            print(f"[write] parquet {path}")
            return
        except Exception as e:
            print(f"[write] parquet failed ({e.__class__.__name__}); csv fallback")
    path.mkdir(parents=True, exist_ok=True)
    _write_csv_rows(df, path / "part-00000.csv")


def save_csv(df, rel: str) -> None:
    path = WAREHOUSE / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_csv_rows(df, path)


def _write_csv_rows(df, path: Path) -> None:
    rows = df.collect()
    fields = df.columns
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in fields})
    print(f"[write] csv     {path}  ({len(rows)} rows)")


def main() -> int:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        users, orders, items = load_ods(spark)
        users.createOrReplaceTempView("ods_users")
        orders.createOrReplaceTempView("ods_orders")
        items.createOrReplaceTempView("ods_order_items")
        print(
            f"[ods] users={users.count()} orders={orders.count()} items={items.count()}"
        )

        run_ods_gates(users, orders, items)

        exec_sql_file(spark, ROOT / "sql" / "dwd.sql")
        exec_sql_file(spark, ROOT / "sql" / "dws.sql")
        exec_sql_file(spark, ROOT / "sql" / "ads.sql")

        dwd_user = spark.table("dwd_dim_user")
        dwd_order = spark.table("dwd_fact_order")
        dwd_item = spark.table("dwd_fact_order_item")
        dws_user = spark.table("dws_user_order_1d")
        dws_cat = spark.table("dws_category_gmv_1d")
        ads_day = spark.table("ads_repurchase_7d").orderBy("dt")
        ads_kpi = spark.table("ads_kpi_overview")

        # ADS CSV is the interview artifact; write it before optional parquet.
        save_csv(ads_kpi, "ads/ads_kpi_overview.csv")
        save_csv(ads_day, "ads/ads_repurchase_7d.csv")
        save_parquet(dwd_user, "dwd/dim_user")
        save_parquet(dwd_order, "dwd/fact_order")
        save_parquet(dwd_item, "dwd/fact_order_item")
        save_parquet(dws_user, "dws/user_order_1d")
        save_parquet(dws_cat, "dws/category_gmv_1d")
        save_parquet(ads_day, "ads/repurchase_7d")

        print("\n=== ADS kpi overview ===")
        ads_kpi.show(truncate=False)
        print("=== ADS 7-day repurchase (window_complete=1, head) ===")
        ads_day.filter("window_complete = 1").show(15, truncate=False)
        print(f"Done. Open {WAREHOUSE / 'ads' / 'ads_kpi_overview.csv'}")
        return 0
    except QualityError as e:
        print(str(e), file=sys.stderr)
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
