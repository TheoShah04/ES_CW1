import sys
sys.stdout.reconfigure(encoding="utf-8")
import pyarrow.dataset as ds

dataset = ds.dataset(
    "C:/Projects/ES/food.parquet",
    format="parquet"
)

barcode = "3017620422003"
table = dataset.to_table(
    filter=ds.field("code") == barcode,
    columns=["code", "product_name"]
)

def extract_product_name(name_list, preferred_lang="en"):
    if not name_list:
        return None

    # exact language match
    for entry in name_list:
        if entry["lang"] == preferred_lang:
            return entry["text"]

    # fallback to 'main'
    for entry in name_list:
        if entry["lang"] == "main":
            return entry["text"]

    # last fallback
    return name_list[0]["text"]

if table.num_rows == 0:
    print("Barcode not found")
else:
    name = table["product_name"][0].as_py()
    print(extract_product_name(name))

