"""Task 4. ETL pipeline: sales_raw.csv -> pandas DataFrame -> SQLite table.

Extract   : read the messy CSV export of the second branch.
Transform : rename/drop columns, fix types, fill missing values,
            normalize text, remove duplicates and invalid rows,
            create the derived column `revenue`.
Load      : write the cleaned DataFrame to the SQLite table `branch_sales`
            using SQLAlchemy.
Verify    : read the table back and print several rows.

Run:  python etl_pipeline.py
"""
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

BASE = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
DB_PATH = BASE / "shop.db"
SOURCE = BASE / "sales_raw.csv"
TABLE = "branch_sales"

RENAME = {
    "Order ID": "order_id", "Customer Name": "customer_name", "City": "city",
    "Product": "product", "Category": "category", "Qty": "qty",
    "Unit Price": "unit_price", "Discount %": "discount",
    "Order Date": "order_date", "Status": "status", "Notes": "notes",
}


def extract(path: Path = SOURCE) -> pd.DataFrame:
    df = pd.read_csv(path)
    print(f"[EXTRACT]   {len(df)} rows x {df.shape[1]} columns read from {path.name}")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    n0 = len(df)
    df = df.rename(columns=RENAME)                 # 1) snake_case names
    df = df.drop(columns=["notes"])                # 2) drop useless column

    df["city"] = df["city"].str.strip().str.title().fillna("Unknown")   # 3) normalize
    df["status"] = df["status"].str.strip().str.lower()

    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")               # 4) fix types
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0.0)
    df["order_date"] = pd.to_datetime(df["order_date"], format="%Y-%m-%d")

    df = df.drop_duplicates()                      # 5) remove duplicate rows
    n_dup = n0 - len(df)
    df = df[df["status"] != "cancelled"]           # 6) keep only real sales
    n_cancel = n0 - n_dup - len(df)
    df = df[df["qty"].notna() & (df["qty"] > 0)]   # 7) rows without a valid qty
    n_bad = n0 - n_dup - n_cancel - len(df)
    df["qty"] = df["qty"].astype(int)

    df["revenue"] = (df["qty"] * df["unit_price"] * (1 - df["discount"])).round(2)  # 8) new column
    df = df.reset_index(drop=True)
    print(f"[TRANSFORM] removed: {n_dup} duplicates, {n_cancel} cancelled, "
          f"{n_bad} invalid-qty rows -> {len(df)} clean rows")
    return df


def load(df: pd.DataFrame, table: str = TABLE) -> None:
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df.to_sql(table, engine, if_exists="replace", index=False)
    print(f"[LOAD]      {len(df)} rows written to table '{table}' in {DB_PATH.name}")


def verify(table: str = TABLE) -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{DB_PATH}")
    back = pd.read_sql_table(table, engine, parse_dates=["order_date"])
    print(f"[VERIFY]    read back {len(back)} rows. First 5 rows:")
    print(back.head().to_string(index=False))
    print(f"[VERIFY]    total revenue in DB: {back['revenue'].sum():,.0f} KZT")
    return back


def main() -> pd.DataFrame:
    raw = extract()
    clean = transform(raw)
    load(clean)
    return verify()


if __name__ == "__main__":
    main()
