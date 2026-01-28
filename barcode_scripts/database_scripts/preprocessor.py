import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

INPUT_FILE = "C:/Projects/ES/food.parquet"
OUTPUT_FILE = "C:/Projects/ES/food_pq.parquet"

pf = pq.ParquetFile(INPUT_FILE)
writer = None

 # ['additives_n', 'element', 'element', 'element', 'brands', 'categories', 'element', 'ciqual_food_code', 'agribalyse_food_code', 'agribalyse_proxy_food_code', 'element', 'element', 'element', 'code', 'compared_to_category', 'complete', 'completeness', 'element', 'element', 'created_t', 'creator', 'element', 'element', 'element', 'element', 'ecoscore_data', 'ecoscore_grade', 'ecoscore_score', 'element', 'element', 'element', 'emb_codes', 'element', 'element', 'lang', 'text', 'key', 'imgid', 'rev', 'h', 'w', 'h', 'w', 'h', 'w', 'h', 'w', 'uploaded_t', 'uploader', 'element', 'element', 'ingredients_from_palm_oil_n', 'ingredients_n', 'element', 'ingredients_percent_analysis', 'element', 'lang', 'text', 'ingredients_with_specified_percent_n', 'ingredients_with_unspecified_percent_n', 'ingredients_without_ciqual_codes_n', 'element', 'ingredients', 'known_ingredients_n', 'element', 'labels', 'lang', 'element', 'element', 'last_editor', 'last_image_t', 'last_modified_by', 'last_modified_t', 'last_updated_t', 'link', 'element', 'element', 'manufacturing_places', 'max_imgid', 'element', 'element', 'new_additives_n', 'no_nutrition_data', 'nova_group', 'element', 'nova_groups', 'element', 'element', 'name', 'value', '100g', 'serving', 'unit', 'prepared_value', 'prepared_100g', 'prepared_serving', 'prepared_unit', 'nutriscore_grade', 'nutriscore_score', 'nutrition_data_per', 'obsolete', 'element', 'origins', 'field_name', 'timestamp', 'owner', 'packagings_complete', 'element', 'element', 'element', 'lang', 'text', 'packaging', 'material', 'number_of_units', 'quantity_per_unit', 'quantity_per_unit_unit', 'quantity_per_unit_value', 'recycling', 'shape', 'weight_measured', 'element', 'popularity_key', 'element', 'lang', 'text', 'product_quantity_unit', 'product_quantity', 'element', 'quantity', 'rev', 'scans_n', 'serving_quantity', 'serving_size', 'element', 'element', 'stores', 'element', 'unique_scans_n', 'unknown_ingredients_n', 'element', 'element', 'with_non_nutritive_sweeteners', 'with_sweeteners']

for rg in range(pf.num_row_groups):
    table = pf.read_row_group(rg, columns=["code", "product_name", "categories", "weight_measured", "product_quantity", "number_of_units" "product_quantity_unit", "quantity_per_unit_value", "quantity_per_unit_unit"])
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
