import sqlite3
DB_FILE = "C:/Projects/ES/processed.db"
# DB_FILE = "/home/pi/processed.db"

barcode = "3017620422003"

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute("SELECT name FROM products WHERE code=?", (barcode,))
result = cur.fetchone()
if result:
    print("Product name:", result[0])
else:
    print("Barcode not found.")

conn.close()
