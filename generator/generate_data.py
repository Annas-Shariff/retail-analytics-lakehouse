"""
Retail Analytics Lakehouse - synthetic data generator.

Simulates a source system that emits a daily batch of files:

    data/day_01/  customers.csv  products.csv  orders.csv
    data/day_02/  customers.csv  products.csv  orders.csv
    ...

Day 1 is the initial full load. Each later day:
  - orders.csv   = ONLY that day's new orders   -> feeds incremental load
  - customers.csv = a fresh full snapshot in which a few customers
                    have changed city             -> feeds SCD Type 2
  - products.csv = unchanged snapshot (dimensions rarely change here)

Usage:
    python generate_data.py --day 1          # initial load
    python generate_data.py --day 2          # next daily batch
    python generate_data.py --day 2 --orders 500 --city-changes 5
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

# ---- config -----------------------------------------------------------------
SEED = 42                     # fixed seed => reproducible data
N_CUSTOMERS = 200
N_PRODUCTS = 50
ORDERS_PER_DAY = 300
CITY_CHANGES_PER_DAY = 3      # how many customers move city each later day

# Indian cities + their state, so we can build "revenue by state" KPIs later
CITIES = [
    ("Bangalore", "Karnataka"), ("Mumbai", "Maharashtra"),
    ("Delhi", "Delhi"), ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"), ("Pune", "Maharashtra"),
    ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"), ("Kochi", "Kerala"),
]

CATEGORIES = {
    "Electronics": ["Sony", "Samsung", "Apple", "Boat", "OnePlus"],
    "Apparel": ["Nike", "Adidas", "Puma", "Levis", "HRX"],
    "Home": ["IKEA", "Prestige", "Milton", "Cello", "Wonderchef"],
    "Beauty": ["Lakme", "Maybelline", "Nykaa", "Mamaearth", "Dove"],
    "Grocery": ["Tata", "Amul", "Nestle", "Britannia", "ITC"],
}

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
START_DATE = datetime(2026, 1, 1)   # "day 1" business date

fake = Faker("en_IN")


def day_dir(day: int) -> Path:
    d = DATA_ROOT / f"day_{day:02d}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {len(rows):>5} rows -> {path.relative_to(DATA_ROOT.parent)}")


# ---- dimension builders -----------------------------------------------------
def build_customers() -> list[dict]:
    """Full customer dimension (day 1 state)."""
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        city, state = random.choice(CITIES)
        customers.append({
            "customer_id": f"C{i:04d}",
            "name": fake.name(),
            "city": city,
            "state": state,
            "age": random.randint(18, 70),
            "gender": random.choice(["M", "F"]),
            "signup_date": fake.date_between(
                start_date="-3y", end_date="today").isoformat(),
        })
    return customers


def build_products() -> list[dict]:
    products = []
    pid = 1
    for category, brands in CATEGORIES.items():
        for _ in range(N_PRODUCTS // len(CATEGORIES)):
            brand = random.choice(brands)
            products.append({
                "product_id": f"P{pid:04d}",
                "product_name": f"{brand} {fake.word().title()}",
                "category": category,
                "brand": brand,
                "price": round(random.uniform(99, 49999), 2),
            })
            pid += 1
    return products


def build_orders(day: int, customers, products, n_orders: int) -> list[dict]:
    """One day's worth of order events."""
    price_by_pid = {p["product_id"]: p["price"] for p in products}
    business_date = START_DATE + timedelta(days=day - 1)
    orders = []
    # order_id is globally unique across days: day*100000 + seq
    for seq in range(1, n_orders + 1):
        cust = random.choice(customers)
        prod = random.choice(products)
        qty = random.randint(1, 5)
        # random time within the business day
        ts = business_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        orders.append({
            "order_id": f"O{day * 100000 + seq:08d}",
            "customer_id": cust["customer_id"],
            "product_id": prod["product_id"],
            "quantity": qty,
            "amount": round(price_by_pid[prod["product_id"]] * qty, 2),
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return orders


# ---- snapshot state helpers -------------------------------------------------
def load_prev_customers(day: int) -> list[dict] | None:
    """Read the most recent previous day's customer snapshot, if any."""
    for prev in range(day - 1, 0, -1):
        path = DATA_ROOT / f"day_{prev:02d}" / "customers.csv"
        if path.exists():
            with path.open() as f:
                return list(csv.DictReader(f))
    return None


def load_products(day: int) -> list[dict] | None:
    for prev in range(day, 0, -1):
        path = DATA_ROOT / f"day_{prev:02d}" / "products.csv"
        if path.exists():
            with path.open() as f:
                rows = list(csv.DictReader(f))
            for r in rows:            # csv reads everything as str
                r["price"] = float(r["price"])
            return rows
    return None


def apply_city_changes(customers: list[dict], n_changes: int) -> list[dict]:
    """Move a few customers to a different city (drives SCD Type 2)."""
    movers = random.sample(customers, min(n_changes, len(customers)))
    for c in movers:
        choices = [x for x in CITIES if x[0] != c["city"]]
        new_city, new_state = random.choice(choices)
        print(f"  city change: {c['customer_id']} "
              f"{c['city']} -> {new_city}")
        c["city"], c["state"] = new_city, new_state
    return customers


# ---- main -------------------------------------------------------------------
def generate(day: int, n_orders: int, n_city_changes: int) -> None:
    random.seed(SEED + day)       # seed varies per day but is reproducible
    Faker.seed(SEED + day)
    out = day_dir(day)
    print(f"Generating day {day} -> {out}")

    if day == 1:
        customers = build_customers()
        products = build_products()
    else:
        prev = load_prev_customers(day)
        products = load_products(day)
        if prev is None or products is None:
            raise SystemExit(
                f"Day {day} needs an earlier day to exist. Run --day 1 first.")
        # normalise types coming back from CSV
        for c in prev:
            c["age"] = int(c["age"])
        customers = apply_city_changes(prev, n_city_changes)

    orders = build_orders(day, customers, products, n_orders)

    write_csv(out / "customers.csv",
              ["customer_id", "name", "city", "state", "age", "gender",
               "signup_date"],
              [[c[k] for k in ("customer_id", "name", "city", "state",
                               "age", "gender", "signup_date")]
               for c in customers])
    write_csv(out / "products.csv",
              ["product_id", "product_name", "category", "brand", "price"],
              [[p[k] for k in ("product_id", "product_name", "category",
                               "brand", "price")]
               for p in products])
    write_csv(out / "orders.csv",
              ["order_id", "customer_id", "product_id", "quantity", "amount",
               "order_ts"],
              [[o[k] for k in ("order_id", "customer_id", "product_id",
                               "quantity", "amount", "order_ts")]
               for o in orders])
    print("Done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Retail lakehouse data generator")
    p.add_argument("--day", type=int, required=True, help="business day number (1 = initial load)")
    p.add_argument("--orders", type=int, default=ORDERS_PER_DAY, help="orders to generate this day")
    p.add_argument("--city-changes", type=int, default=CITY_CHANGES_PER_DAY, help="customers who move city (day > 1)")
    args = p.parse_args()
    generate(args.day, args.orders, args.city_changes)
