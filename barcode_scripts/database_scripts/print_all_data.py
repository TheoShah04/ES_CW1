import sys
sys.stdout.reconfigure(encoding="utf-8")
import pyarrow.parquet as pq

# -------------------------------
# Configuration
# -------------------------------
PARQUET_FILE = "C:/Projects/ES/food.parquet"
BARCODE = "3017620422003"          # Nutella barcode
BARCODE = "03249833"

# -------------------------------
# Open Parquet file
# -------------------------------
pf = pq.ParquetFile(PARQUET_FILE)

found = False

# Loop over row groups
for rg in range(pf.num_row_groups):
    # Read all columns for this row group
    table = pf.read_row_group(rg)
    
    codes = table["code"].to_pylist()
    
    # Search for the Nutella barcode
    for i, code in enumerate(codes):
        if code == BARCODE:
            print("------ Nutella Full Info ------")
            for col in table.column_names:
                value = table[col][i].as_py()
                print(f"{col}: {value}")
            found = True
            break
    
    if found:
        break

if not found:
    print("Nutella barcode not found in this file.")
