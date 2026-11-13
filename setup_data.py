"""Setup for Practice 6.
Creates:
  1) shop.db  - SQLite database with 4 related tables (customers, products, orders, order_items)
  2) sales_raw.csv - a messy CSV export from a "second branch", used as the ETL source.
Run:  python setup_data.py
"""
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

BASE = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
rng = np.random.default_rng(42)

# ----------------------------------------------------------------------
# 1. Reference tables
# ----------------------------------------------------------------------
customers = pd.DataFrame({
    "customer_id": range(1, 13),
    "name": ["Aigerim S.", "Daulet K.", "Madina T.", "Arman B.", "Aliya N.",
             "Nurlan Zh.", "Dana A.", "Yerlan M.", "Asem O.", "Timur R.",
             "Gulnaz E.", "Bekzat U."],
    "city": ["Almaty", "Astana", "Shymkent", "Karaganda", "Aktobe", "Almaty",
             "Astana", "Almaty", "Shymkent", "Karaganda", None, "Astana"],
    "segment": ["Consumer", "Corporate", "Consumer", "Small Business", "Consumer",
                "Corporate", "Consumer", "Small Business", "Consumer",
                "Corporate", "Consumer", "Small Business"],
    "signup_date": (pd.to_datetime("2024-01-01")
                    + pd.to_timedelta(rng.integers(0, 700, 12), unit="D")),
})

products = pd.DataFrame({
    "product_id": range(1, 11),
    "product_name": ['Laptop 15"', "Wireless Mouse", "Mechanical Keyboard",
                     'Monitor 24"', "USB-C Hub", "Webcam HD", "Desk Lamp",
                     "Notebook Pack", "Headphones", "SSD 1TB"],
    "category": ["Electronics", "Accessories", "Accessories", "Electronics",
                 "Accessories", "Electronics", "Office", "Office",
                 "Electronics", "Electronics"],
    "unit_price": [450000, 8000, 25000, 95000, 12000,
                   18000, 7000, 2500, 30000, 40000],  # KZT
})

# ----------------------------------------------------------------------
# 2. Orders and order items
# ----------------------------------------------------------------------
N_ORDERS = 80
orders = pd.DataFrame({
    "order_id": range(1001, 1001 + N_ORDERS),
    "customer_id": rng.integers(1, 13, N_ORDERS),
    "order_date": (pd.to_datetime("2026-01-01")
                   + pd.to_timedelta(rng.integers(0, 181, N_ORDERS), unit="D")),
    "status": rng.choice(["completed", "cancelled", "returned"],
                         N_ORDERS, p=[0.80, 0.12, 0.08]),
})

rows, item_id = [], 1
for oid in orders["order_id"]:
    k = int(rng.integers(1, 4))                      # 1..3 items per order
    pids = rng.choice(products["product_id"], size=k, replace=False)
    for pid in pids:
        disc = rng.choice([0.0, 0.05, 0.10, 0.15, np.nan],
                          p=[0.50, 0.20, 0.10, 0.10, 0.10])
        rows.append([item_id, oid, int(pid), int(rng.integers(1, 6)), disc])
        item_id += 1
order_items = pd.DataFrame(rows, columns=["item_id", "order_id", "product_id",
                                          "quantity", "discount"])

# ----------------------------------------------------------------------
# 3. Create the database (DDL with foreign keys, then INSERT via pandas)
# ----------------------------------------------------------------------
engine = create_engine(f"sqlite:///{BASE / 'shop.db'}")
ddl = [
    "DROP TABLE IF EXISTS order_items",
    "DROP TABLE IF EXISTS orders",
    "DROP TABLE IF EXISTS products",
    "DROP TABLE IF EXISTS customers",
    """CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,
        city        TEXT,
        segment     TEXT NOT NULL,
        signup_date TEXT)""",
    """CREATE TABLE products (
        product_id   INTEGER PRIMARY KEY,
        product_name TEXT NOT NULL,
        category     TEXT NOT NULL,
        unit_price   REAL NOT NULL)""",
    """CREATE TABLE orders (
        order_id    INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
        order_date  TEXT NOT NULL,
        status      TEXT NOT NULL)""",
    """CREATE TABLE order_items (
        item_id    INTEGER PRIMARY KEY,
        order_id   INTEGER NOT NULL REFERENCES orders(order_id),
        product_id INTEGER NOT NULL REFERENCES products(product_id),
        quantity   INTEGER NOT NULL,
        discount   REAL)""",
]
with engine.begin() as conn:
    conn.execute(text("PRAGMA foreign_keys = ON"))
    for stmt in ddl:
        conn.execute(text(stmt))

db_customers = customers.assign(signup_date=customers["signup_date"].dt.strftime("%Y-%m-%d"))
db_orders = orders.assign(order_date=orders["order_date"].dt.strftime("%Y-%m-%d"))
db_customers.to_sql("customers", engine, if_exists="append", index=False)
products.to_sql("products", engine, if_exists="append", index=False)
db_orders.to_sql("orders", engine, if_exists="append", index=False)
order_items.to_sql("order_items", engine, if_exists="append", index=False)

with engine.connect() as conn:
    counts = {t: conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
              for t in ["customers", "products", "orders", "order_items"]}
print("Database created:", BASE / "shop.db")
print("Rows per table:", counts)

# ----------------------------------------------------------------------
# 4. Messy CSV export from the second branch -> ETL source
# ----------------------------------------------------------------------
N = 120
pidx = rng.integers(0, len(products), N)
raw = pd.DataFrame({
    "Order ID": [f"B-{2001 + i}" for i in range(N)],
    "Customer Name": rng.choice(customers["name"], N),
    "City": rng.choice(["Almaty", "almaty", "Astana", " astana ", "Shymkent",
                        "Karaganda", None], N,
                       p=[0.25, 0.10, 0.20, 0.10, 0.15, 0.10, 0.10]),
    "Product": products["product_name"].to_numpy()[pidx],
    "Category": products["category"].to_numpy()[pidx],
    "Qty": rng.integers(1, 6, N).astype(float),
    "Unit Price": products["unit_price"].to_numpy()[pidx].astype(float),
    "Discount %": rng.choice([0.0, 0.05, 0.10, np.nan], N,
                             p=[0.45, 0.20, 0.10, 0.25]),
    "Order Date": (pd.to_datetime("2026-07-01")
                   + pd.to_timedelta(rng.integers(0, 92, N), unit="D")).strftime("%Y-%m-%d"),
    "Status": rng.choice(["Completed", "completed", "Cancelled", "Returned"],
                         N, p=[0.55, 0.20, 0.15, 0.10]),
    "Notes": [None] * N,                      # useless empty column
})
raw.loc[rng.choice(N, 5, replace=False), "Qty"] = np.nan   # missing quantities
raw = pd.concat([raw, raw.iloc[[7, 40, 88]]], ignore_index=True)  # 3 duplicate rows
raw.to_csv(BASE / "sales_raw.csv", index=False)

print("CSV source created:", BASE / "sales_raw.csv", "| shape:", raw.shape)
print("Missing values in CSV:\n", raw.isna().sum().to_string())
