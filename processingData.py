import os
import pandas as pd
import gc
import pyarrow.parquet as pq
from logger import get_logger
from tqdm import tqdm
from RedisUtils.redisProcessing import load_or_create_vendors, save_vendor, get_model_mapping, update_model_mapping

logger = get_logger("Filling Data in Redis")

# def process_chunk(chunk, predefined_vendors, model_mapping):
#     """
#     Process a chunk of the DataFrame to handle make and model processing.
#     """
#     try:
#         chunk = chunk.astype(str).apply(lambda x: x.str.lower().fillna(""))
#         chunk['ua'] = chunk['ua'].str.lower()
#         chunk['device_model'] = chunk['device_model'].str.lower()
#         chunk['device_vendor'] = chunk['device_vendor'].str.lower()
#         chunk['device_height'] = chunk['device_height'].astype(str).str.lower()
#         chunk['device_width'] = chunk['device_width'].astype(str).str.lower()
#     except Exception as e:
#         logger.error(f"Error processing columns: {e}")
#         return

#     for index, row in chunk.iterrows():
#         try:
#             model = row['device_model'] if row['device_model'] else ""
#             vendor = str(row['device_vendor']) if row['device_vendor'] else ""
#             height = row.get('device_height', "")
#             width = row.get('device_width', "")

#             if vendor in predefined_vendors:
#                 model = model.replace(vendor, "").strip("_ .").replace("_", " ").replace(".", " ").strip()

#             if not vendor or (len(vendor) <= 3 and vendor != "lg"):
#                 vendor = extract_vendor_from_ua(row['ua'], predefined_vendors)
#                 if vendor and vendor not in predefined_vendors:
#                     save_vendor(vendor)

#             if model in model_mapping:
#                 # Update existing entry only if new values are provided
#                 existing_entry = model_mapping[model]
#                 updated_entry = {
#                     "vendor": vendor or existing_entry.get("vendor"),
#                     "height": height or existing_entry.get("height"),
#                     "width": width or existing_entry.get("width")
#                 }
#                 normalized_existing = {k: str(v).strip().lower() for k, v in existing_entry.items()}
#                 normalized_updated = {k: str(v).strip().lower() for k, v in updated_entry.items()}
#                 # if updated_entry != existing_entry:
#                 #     update_model_mapping(model, updated_entry)
#                 if normalized_existing != normalized_updated:
#                     update_model_mapping(model, updated_entry)
#             else:
#                 update_model_mapping(model, {"vendor": vendor, "height": height, "width": width})

#         except Exception as e:
#             logger.error(f"Error processing row: {e}")

def process_chunk(chunk, predefined_vendors,model_mapping):
    """
    Process a chunk of the DataFrame to handle make and model processing.
    """
    try:
        chunk = chunk.astype(str).apply(lambda x: x.str.lower().fillna(""))
        chunk['ua'] = chunk['ua'].str.lower()
        chunk['device_model'] = chunk['device_model'].str.lower()
        chunk['device_vendor'] = chunk['device_vendor'].str.lower()
        chunk['device_height'] = chunk['device_height'].astype(str).str.lower()
        chunk['device_width'] = chunk['device_width'].astype(str).str.lower()
    except Exception as e:
        logger.error(f"Error processing columns: {e}")
        return

    for index, row in chunk.iterrows():
        try:
            model = row['device_model'] if row['device_model'] else ""
            vendor = str(row['device_vendor']) if row['device_vendor'] else ""
            height = row.get('device_height', "")
            width = row.get('device_width', "")

            if vendor in predefined_vendors:
                model = model.replace(vendor, "").strip("_ .").replace("_", " ").replace(".", " ").strip()

            if not vendor or (len(vendor) <= 3 and vendor != "lg"):
                vendor = extract_vendor_from_ua(row['ua'], predefined_vendors)
                if vendor and vendor not in predefined_vendors:
                    save_vendor(vendor)

            # Fetch latest model mapping dynamically instead of using stale data
            existing_entry = get_model_mapping().get(model, {})

            updated_entry = {
                "vendor": vendor or existing_entry.get("vendor"),
                "height": height or existing_entry.get("height"),
                "width": width or existing_entry.get("width")
            }

            normalized_existing = {k: str(v).strip().lower() for k, v in existing_entry.items()}
            normalized_updated = {k: str(v).strip().lower() for k, v in updated_entry.items()}

            if normalized_existing != normalized_updated:
                update_model_mapping(model, updated_entry)

        except Exception as e:
            logger.error(f"Error processing row: {e}")

def extract_vendor_from_ua(ua, predefined_vendors, current_vendor=None):
    """
    Extract the vendor from the 'ua' field using the predefined vendors list.
    """
    if pd.notna(ua):
        ua_section = ua.split(")")[0] if ")" in ua else ua

        for vendor in predefined_vendors:
            if vendor in ua_section:
                if current_vendor is not None and "android" in current_vendor and vendor == "apple":
                    continue
                elif current_vendor is not None and "apple" in current_vendor and vendor == "android":
                    continue
                return vendor

    return None

def process_parquet_files():
    predefined_vendors = load_or_create_vendors()
    model_mapping = get_model_mapping()

    baseDir="temp"

    if not baseDir:
        logger.error("Base dir not found")

    for root, _, files in os.walk(baseDir):
        for file_name in files:
            if file_name.endswith(".parquet"):
                file_path = os.path.join(root, file_name)

                try:
                    parquet_file = pq.ParquetFile(file_path)
                    for row_group_idx in tqdm(range(parquet_file.num_row_groups), desc=f"Processing {file_name}"):
                        chunk = parquet_file.read_row_group(row_group_idx).to_pandas()
                        process_chunk(chunk, predefined_vendors, model_mapping)

                except Exception as e:
                    logger.error(f"Error processing file {file_name}: {e}")

                gc.collect()


