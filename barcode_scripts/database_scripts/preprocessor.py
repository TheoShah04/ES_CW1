import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

INPUT_FILE = "C:/Projects/ES/food.parquet"
OUTPUT_FILE = "C:/Projects/ES/processed.parquet"

pf = pq.ParquetFile(INPUT_FILE)
writer = None

for rg in range(pf.num_row_groups):
    table = pf.read_row_group(rg, columns=["code", "product_name"])
    codes = table["code"]
    names_list = table["product_name"]  # list<struct<lang,text>>

    # Flatten the list of structs
    exploded_names = pc.list_flatten(names_list)

    # Extract fields
    langs = pc.struct_field(exploded_names, "lang")
    texts = pc.struct_field(exploded_names, "text")

    # Map flattened names back to their parent row (barcode)
    parent_idx = pc.list_parent_indices(names_list)
    repeated_codes = pc.take(codes, parent_idx)

    # Build table: code + lang + text
    exploded_table = pa.table({
        "code": repeated_codes,
        "lang": langs,
        "text": texts
    })

    # Filter English first
    mask_en = pc.equal(exploded_table["lang"], "en")
    en_table = exploded_table.filter(mask_en)

    # Fallback to 'main' if English missing
    mask_main = pc.equal(exploded_table["lang"], "main")
    main_table = exploded_table.filter(mask_main)

    # Merge: prefer English, else main
    en_codes = set(en_table["code"].to_pylist())

    # remove None values
    en_codes = [c for c in en_codes if c is not None]

    if en_codes:
        is_in_en = pc.is_in(main_table["code"], pa.array(en_codes))
        not_in_en = pc.invert(is_in_en)
        fallback_rows = main_table.filter(not_in_en)
    else:
        fallback_rows = main_table

    merged_table = pa.concat_tables([en_table, fallback_rows])

    # Keep only code + text
    out = merged_table.select(["code", "text"])

    # Write to Parquet
    if writer is None:
        writer = pq.ParquetWriter(OUTPUT_FILE, out.schema, compression="zstd")
    writer.write_table(out)

if writer:
    writer.close()

print("Preprocessing complete.")
