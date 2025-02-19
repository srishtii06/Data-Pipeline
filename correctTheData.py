

import os
import pandas as pd
import gc
import pyarrow.parquet as pq
from logger import get_logger
# from tqdm import tqdm
# from RedisUtils.redisProcessing import load_or_create_vendors, get_model_mapping
import numpy as np
import pandas as pd
import redis
import json
import logging

# Set up logging
logger = logging.getLogger(__name__)

def correct_data_with_mapping(chunk, predefined_vendors, model_mapping,r):
    """
    Correct data in a chunk based on Redis model mapping, using bulk queries.
    """
    corrected_rows = []

    # Extract all unique models in bulk for Redis lookup
    unique_models = chunk['device_model'].dropna().str.strip().str.lower().unique()
    redis_keys = [f"model_mapping:{model}" for model in unique_models]

    # Bulk fetch data from Redis
    redis_data = r.mget(redis_keys)
    model_mapping_bulk = {
        model: json.loads(data) if data else None 
        for model, data in zip(unique_models, redis_data)
    }

    for _, row in chunk.iterrows():
        try:
            model = row['device_model'].strip().lower()
            vendor = row['device_vendor'].strip().lower() if pd.notna(row['device_vendor']) else ""
            os_version = row.get('os_version', "")
            normalized_os_version = os_version.split('.')[0] if pd.notna(os_version) else ""

            updated_row = row.to_dict()  # Convert the row to a dictionary to retain all columns

            if "android" in model or "android" in vendor:
                extracted_vendor = extract_vendor_from_ua(row['ua'], predefined_vendors, current_vendor=vendor)
                updated_row.update({
                    "device_vendor": extracted_vendor or vendor,
                    "normalized_os_version": normalized_os_version
                })
                corrected_rows.append(updated_row)
                continue

            for predefined_vendor in predefined_vendors:
                if predefined_vendor in model:
                    model = model.replace(predefined_vendor, "").strip("_ .").replace("_", " ").replace(".", " ").strip()
                    if not vendor:
                        vendor = predefined_vendor
                    break

            redis_entry = model_mapping_bulk.get(model, None)

            if redis_entry:
                updated_row.update({
                    "device_vendor": redis_entry.get('vendor', vendor),
                    "device_model": model,
                    "device_height": redis_entry.get('height', row['device_height']),
                    "device_width": redis_entry.get('width', row['device_width']),
                    "normalized_os_version": normalized_os_version
                })
            else:
                updated_row.update({
                    "device_vendor": vendor,
                    "device_model": model,
                    "normalized_os_version": normalized_os_version
                })

            corrected_rows.append(updated_row)
        except Exception as e:
            logger.error(f"Error processing row in data correction: {e}")

    return pd.DataFrame(corrected_rows)


def extract_vendor_from_ua(ua, predefined_vendors, current_vendor=None):
    """Extract vendor from the 'ua' field using predefined vendor names."""
    if pd.notna(ua):
        ua_section = ua.split(")")[0] if ")" in ua else ua
        for vendor in predefined_vendors:
            if vendor in ua_section:
                if current_vendor and "android" in current_vendor and vendor == "apple":
                    continue
                elif current_vendor and "apple" in current_vendor and vendor == "android":
                    continue
                return vendor
    return None
