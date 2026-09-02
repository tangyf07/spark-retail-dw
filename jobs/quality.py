"""Hard quality gates. Any violation fails the job (non-zero exit)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityError(RuntimeError):
    pass


def _fail(name: str, n: int, hint: str = "") -> None:
    extra = f" | {hint}" if hint else ""
    raise QualityError(f"QUALITY FAIL [{name}]: {n} bad row(s){extra}")


def assert_not_null(df: DataFrame, cols: list[str], name: str) -> None:
    cond = None
    for c in cols:
        piece = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        cond = piece if cond is None else (cond | piece)
    n = df.filter(cond).count()
    if n:
        _fail(name, n, f"null/empty in {cols}")


def assert_unique(df: DataFrame, cols: list[str], name: str) -> None:
    n = df.groupBy(*cols).count().filter(F.col("count") > 1).count()
    if n:
        _fail(name, n, f"duplicate PK {cols}")


def assert_values_in(df: DataFrame, col: str, allowed: set[str], name: str) -> None:
    n = df.filter(~F.col(col).isin(list(allowed)) | F.col(col).isNull()).count()
    if n:
        _fail(name, n, f"{col} not in {sorted(allowed)}")


def assert_non_negative(df: DataFrame, col: str, name: str) -> None:
    n = df.filter(F.col(col).isNull() | (F.col(col) < 0)).count()
    if n:
        _fail(name, n, f"{col} < 0 or null")


def assert_positive(df: DataFrame, col: str, name: str) -> None:
    n = df.filter(F.col(col).isNull() | (F.col(col) <= 0)).count()
    if n:
        _fail(name, n, f"{col} <= 0 or null")


def assert_fk(child: DataFrame, parent: DataFrame, key: str, name: str) -> None:
    n = child.join(parent, on=key, how="left_anti").count()
    if n:
        _fail(name, n, f"orphan {key}")


def assert_amount_match(orders: DataFrame, items: DataFrame, name: str) -> None:
    """Paid orders: header pay_amount must equal sum(item.amount) within 0.01."""
    paid = orders.filter(F.col("status").isin("paid", "shipped", "completed"))
    summed = items.groupBy("order_id").agg(F.round(F.sum("amount"), 2).alias("item_sum"))
    bad = (
        paid.join(summed, "order_id", "left")
        .filter(
            F.col("item_sum").isNull()
            | (F.abs(F.col("pay_amount") - F.col("item_sum")) > 0.01)
        )
    )
    n = bad.count()
    if n:
        _fail(name, n, "pay_amount != sum(order_items.amount)")


def run_ods_gates(users: DataFrame, orders: DataFrame, items: DataFrame) -> None:
    assert_not_null(users, ["user_id", "register_dt", "city", "gender", "channel"], "ods_users.required")
    assert_unique(users, ["user_id"], "ods_users.pk")
    assert_values_in(users, "gender", {"M", "F"}, "ods_users.gender")
    assert_values_in(users, "channel", {"app", "wechat", "web"}, "ods_users.channel")

    assert_not_null(orders, ["order_id", "user_id", "order_ts", "status", "pay_amount", "dt"], "ods_orders.required")
    assert_unique(orders, ["order_id"], "ods_orders.pk")
    assert_values_in(
        orders,
        "status",
        {"unpaid", "paid", "shipped", "completed", "cancelled"},
        "ods_orders.status",
    )
    assert_non_negative(orders, "pay_amount", "ods_orders.pay_amount")
    assert_fk(orders, users, "user_id", "ods_orders.fk_user")

    assert_not_null(
        items,
        ["order_item_id", "order_id", "sku_id", "category", "qty", "unit_price", "amount"],
        "ods_items.required",
    )
    assert_unique(items, ["order_item_id"], "ods_items.pk")
    assert_positive(items, "qty", "ods_items.qty")
    assert_non_negative(items, "unit_price", "ods_items.unit_price")
    assert_non_negative(items, "amount", "ods_items.amount")
    assert_fk(items, orders, "order_id", "ods_items.fk_order")
    assert_amount_match(orders, items, "ods_orders.amount_vs_items")

    print("[quality] ODS gates passed")
