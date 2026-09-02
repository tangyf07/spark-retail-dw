#!/usr/bin/env python3
"""Generate a small synthetic ODS sample for the Spark retail DW demo (offline)."""
from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
START = date(2026, 7, 1)
END = date(2026, 8, 15)

CITIES = ["哈尔滨", "北京", "上海", "深圳", "杭州", "成都"]
CHANNELS = ["app", "wechat", "web"]
PAY_CHANNELS = ["alipay", "wechat", "card"]
STATUSES_PAID = ["paid", "shipped", "completed"]
STATUSES_OTHER = ["unpaid", "cancelled"]

SKUS = [
    ("SKU01", "蓝牙耳机", "数码", 129.00),
    ("SKU02", "机械键盘", "数码", 299.00),
    ("SKU03", "纯棉T恤", "服饰", 79.00),
    ("SKU04", "牛仔裤", "服饰", 189.00),
    ("SKU05", "坚果礼盒", "食品", 68.00),
    ("SKU06", "咖啡豆", "食品", 45.00),
    ("SKU07", "香薰蜡烛", "家居", 39.00),
    ("SKU08", "四件套", "家居", 199.00),
    ("SKU09", "面霜", "美妆", 159.00),
    ("SKU10", "口红", "美妆", 99.00),
]

def main() -> None:
    rng = random.Random(SEED)
    out = Path(__file__).resolve().parents[1] / "data" / "ods"
    out.mkdir(parents=True, exist_ok=True)

    users = []
    for i in range(1, 49):
        uid = f"U{i:04d}"
        reg = START - timedelta(days=rng.randint(10, 120))
        users.append(
            {
                "user_id": uid,
                "register_dt": reg.isoformat(),
                "city": rng.choice(CITIES),
                "gender": rng.choice(["M", "F"]),
                "channel": rng.choice(CHANNELS),
            }
        )
        # persona: 0 loyal, 1 occasional, 2 one-shot
        users[-1]["_persona"] = 0 if i <= 14 else (1 if i <= 30 else 2)

    orders = []
    items = []
    oid = 1
    iid = 1

    loyal = [u for u in users if u["_persona"] == 0]
    occasional = [u for u in users if u["_persona"] == 1]
    oneshot = [u for u in users if u["_persona"] == 2]

    # Loyal: 3-5 paid orders, many within 7 days
    for u in loyal:
        n = rng.randint(3, 5)
        t = START + timedelta(days=rng.randint(0, 12))
        for k in range(n):
            if k > 0:
                t = t + timedelta(days=rng.randint(2, 6))  # within 7d
            if t > END:
                break
            oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=True)

    # Occasional: 2-3 orders, gaps often > 7d
    for u in occasional:
        n = rng.randint(2, 3)
        t = START + timedelta(days=rng.randint(0, 20))
        for k in range(n):
            if k > 0:
                t = t + timedelta(days=rng.randint(8, 18))
            if t > END:
                break
            oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=True)

    # One-shot: exactly 1 paid order
    for u in oneshot:
        t = START + timedelta(days=rng.randint(0, 40))
        oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=True)

    # A handful of cancelled / unpaid noise (~8%)
    noise_users = rng.sample(users, 10)
    for u in noise_users:
        t = START + timedelta(days=rng.randint(0, 40))
        paid = False
        oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=paid)

    write_csv(
        out / "users.csv",
        ["user_id", "register_dt", "city", "gender", "channel"],
        [{k: v for k, v in u.items() if not k.startswith("_")} for u in users],
    )
    write_csv(
        out / "orders.csv",
        ["order_id", "user_id", "order_ts", "status", "pay_amount", "pay_channel", "dt"],
        orders,
    )
    write_csv(
        out / "order_items.csv",
        ["order_item_id", "order_id", "sku_id", "sku_name", "category", "qty", "unit_price", "amount"],
        items,
    )
    print(f"users={len(users)} orders={len(orders)} items={len(items)} -> {out}")


def add_order(rng, user, d, oid, iid, orders, items, paid: bool):
    order_id = f"O{oid:04d}"
    hour = rng.randint(8, 22)
    minute = rng.randint(0, 59)
    ts = datetime(d.year, d.month, d.day, hour, minute, rng.randint(0, 59))
    n_items = rng.choices([1, 2, 3], weights=[0.55, 0.35, 0.10])[0]
    skus = rng.sample(SKUS, n_items)
    total = 0.0
    for sku_id, name, cat, price in skus:
        qty = rng.choice([1, 1, 1, 2])
        amount = round(qty * price, 2)
        total += amount
        items.append(
            {
                "order_item_id": f"I{iid:04d}",
                "order_id": order_id,
                "sku_id": sku_id,
                "sku_name": name,
                "category": cat,
                "qty": qty,
                "unit_price": f"{price:.2f}",
                "amount": f"{amount:.2f}",
            }
        )
        iid += 1
    total = round(total, 2)
    if paid:
        status = rng.choice(STATUSES_PAID)
        pay_channel = rng.choice(PAY_CHANNELS)
        pay_amount = f"{total:.2f}"
    else:
        status = rng.choice(STATUSES_OTHER)
        pay_channel = "" if status == "unpaid" else rng.choice(PAY_CHANNELS)
        pay_amount = "0.00" if status in ("unpaid", "cancelled") else f"{total:.2f}"
        if status == "cancelled":
            pay_amount = "0.00"
    orders.append(
        {
            "order_id": order_id,
            "user_id": user["user_id"],
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "pay_amount": pay_amount,
            "pay_channel": pay_channel,
            "dt": d.isoformat(),
        }
    )
    return oid + 1, iid


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


if __name__ == "__main__":
    main()
