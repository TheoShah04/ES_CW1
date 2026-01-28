import sys
sys.stdout.reconfigure(encoding="utf-8")
import pyarrow.parquet as pq

# -------------------------------
# Configuration
# -------------------------------
PARQUET_FILE = "C:/Projects/ES/food.parquet"
BARCODE = "3017620422003" # Nutella barcode
BARCODE = "7622210018922"  # Oreo barcode
#BARCODE = "5018374888303" # Eggs barcode
#BARCODE = "4088600170633" # Eggs barcode
#BARCODE = "5010525253183" # Milk barcode
#BARCODE = "03266205" # Tesco Carrots
#BARCODE = "5000436589457" # Tesco Milk
#BARCODE = "03249833"
BARCODE = "80052760" # Kinder bueno
BARCODE = "0014500021830" # Brocolli
BARCODE = "50501793225836"

# -------------------------------
# Open Parquet file
# -------------------------------
pf = pq.ParquetFile(PARQUET_FILE)

# -------------------------------
# Loop over row groups and read only relevant columns
# -------------------------------
columns_to_read = [
    "code",
    "product_name",       # or "text" if flattened
    "product_quantity",
    "product_quantity_unit",
    "quantity",
    # "quantity_per_unit_value",
    # "quantity_per_unit_unit",
    "serving_quantity",
    "serving_size"
]

found = False

for rg in range(pf.num_row_groups):
    table = pf.read_row_group(rg, columns=columns_to_read)
    
    codes = table["code"].to_pylist()
    
    for i, code in enumerate(codes):
        if code == BARCODE:
            for col in columns_to_read[1:]:
                # Some columns may not exist in your flattened file; handle safely
                value = table[col][i].as_py() if col in table.column_names else None
                print(f"{col}: {value}")
            found = True
            break
    
    if found:
        break

if not found:
    print("Barcode not found in this file.")
