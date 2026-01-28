import sqlite3
import pyarrow.parquet as pq

# -------------------------------
# Configuration
# -------------------------------
PARQUET_FILE = "C:/Projects/ES/processed.parquet"  # already flattened: code + text
DB_FILE = "C:/Projects/ES/processed.db"
# -------------------------------
# Open Parquet file
# -------------------------------
pf = pq.ParquetFile(PARQUET_FILE)

# -------------------------------
# Create persistent SQLite database
# -------------------------------
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS products")
cur.execute("CREATE TABLE products(code TEXT PRIMARY KEY, name TEXT)")

# -------------------------------
# Process row groups sequentially
# -------------------------------
for rg in range(pf.num_row_groups):
    table = pf.read_row_group(rg, columns=["code", "text"])
    
    codes = table["code"].to_pylist()
    names = table["text"].to_pylist()
    
    # Insert into SQLite, overwrite duplicates if any
    cur.executemany("INSERT OR REPLACE INTO products VALUES (?, ?)", zip(codes, names))

conn.commit()
conn.close()
print(f"SQLite database '{DB_FILE}' created successfully.")
