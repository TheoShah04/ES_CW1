import sqlite3
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import time

# -------------------------------
# Configuration
# -------------------------------
BARCODE_TO_LOOKUP = "3017620422003"
PARQUET_FILE = "C:/Projects/ES/processed.parquet"
SQLITE_DB = "barcode_lookup.db"

# -------------------------------
# SQLite lookup
# -------------------------------
conn = sqlite3.connect(SQLITE_DB)
cur = conn.cursor()

start_sqlite = time.perf_counter()
cur.execute("SELECT name FROM products WHERE code=?", (BARCODE_TO_LOOKUP,))
result = cur.fetchone()
end_sqlite = time.perf_counter()

sqlite_time = end_sqlite - start_sqlite
sqlite_name = result[0] if result else None
print(f"[SQLite] Name: {sqlite_name}, Time: {sqlite_time*1000:.3f} ms")

# -------------------------------
# PyArrow lookup
# -------------------------------
dataset = ds.dataset(PARQUET_FILE, format="parquet")

start_arrow = time.perf_counter()
# Use predicate pushdown for fast lookup
table = dataset.to_table(
    filter=ds.field("code") == BARCODE_TO_LOOKUP,
    columns=["code", "text"]
)
end_arrow = time.perf_counter()

arrow_time = end_arrow - start_arrow
arrow_name = table["text"][0].as_py() if table.num_rows > 0 else None
print(f"[PyArrow] Name: {arrow_name}, Time: {arrow_time*1000:.3f} ms")
