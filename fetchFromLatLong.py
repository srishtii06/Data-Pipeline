import os
import pandas as pd
import json
import asyncio
import aiohttp
import redis.asyncio as redis
from datetime import datetime, timezone
from logger import get_logger
from config import config
import pyarrow.parquet as pq
import pyarrow as pa
from RedisUtils.redisProcessing import r


logger = get_logger("Filling Data from Redis and Api")

API=config.REVERSE_GEOCODER_API

# PROGRESS_FILE = "progress.json"
# ROW_GROUP_SIZE = 10_000  # Process in row groups of 10,000

SEMAPHORE_LIMIT = 5
semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

# async def get_redis_connection():
#     return await redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)


# def save_progress(file_path, row_count):
#     """Save processing progress to a JSON file."""
#     progress_data = {"file": file_path, "row_count": row_count}
#     with open(PROGRESS_FILE, "w") as f:
#         json.dump(progress_data, f)

# def load_progress():
#     """Load the last saved progress."""
#     if os.path.exists(PROGRESS_FILE):
#         with open(PROGRESS_FILE, "r") as f:
#             return json.load(f)
#     return None

def is_valid_lat_lon(lat, lon):
    lat, lon = float(lat), float(lon) 
    return (-90 <= lat <= 90) and (-180 <= lon <= 180)

async def bulk_fetch_location_data(redis_conn, lat_lon_pairs):
    """Fetch location data from Redis and API (if missing)."""
    redis_keys = [f"location:{lat}:{lon}" for lat, lon in lat_lon_pairs]

    logger.info("Here in Bulk Fetch Location")
    if not redis_keys:  # Ensure there are keys to fetch
        return {}

    # cached_data = await redis_conn.mget(redis_keys)
    cached_data = redis_conn.mget(redis_keys) or []

    result = {}
    missing_coords = []
    missing_keys = []

    for i, data in enumerate(cached_data):
        if data:
            result[lat_lon_pairs[i]] = json.loads(data)
        else:
            missing_coords.append(lat_lon_pairs[i])
            missing_keys.append(redis_keys[i])

    if missing_coords:
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_location_data(session, lat, lon) for lat, lon in missing_coords]
            responses = await asyncio.gather(*tasks)

        # Store only valid API responses in Redis
        redis_pipeline = redis_conn.pipeline()
        for key, coord, response in zip(missing_keys, missing_coords, responses):
            if response:
                redis_pipeline.set(key, json.dumps(response), ex=2592000)  # Expire in 30 days
                result[coord] = response
        redis_pipeline.execute()

    return result

async def fetch_location_data(session, lat, lon):
    """Fetch location data from an external API with a concurrency limit."""
    url = f"{API}/reverse?lat={lat}&lon={lon}"
    logger.info("Here in Fetch Location")
    
    async with semaphore:  # Limit concurrent tasks
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    json_data = await response.json()
                    features = json_data.get("features", [])
                    if features:
                        return features[0].get("properties", {})
                logger.error(f"No API response for ({lat}, {lon})")
        except asyncio.TimeoutError:
            logger.error(f"Timeout error fetching ({lat}, {lon})")
        except Exception as e:
            logger.error(f"Error fetching location for ({lat}, {lon}): {e}")
    return None


def save_transformed_row_group(transformed_data, output_file_path):
    table = pa.Table.from_pandas(pd.DataFrame(transformed_data))
    pq.write_table(table, output_file_path) # Overwrites the file each time a row group is written. More efficient than appending.

async def transform_row_group(row_group, redis_conn):
    expected_columns = ["refId", "reqTime", "reqTimeConverted", "deviceIfa", "os", "osv", "normalized_osv", "ipAddress", 
        "carrier", "connectionType", "device_vendor", "device_model", "device_height", "device_width", 
        "deviceType", "location_type", "latitude", "longitude", "appBundle", "city", "region", "state", 
        "device_country_name", "device_country_code", "zip", "ua", "ssp", "dpidsha1", "dpidmd5" ]

    #  creates a list of valid (latitude, longitude) pairs from row_group, filtering out entries where latitude or longitude is NaN or invalid (out of range)
    lat_lon_pairs = [(row["latitude"], row["longitude"]) for row in row_group
                     if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude"))
                     and is_valid_lat_lon(row["latitude"], row["longitude"])]

    location_data = await bulk_fetch_location_data(redis_conn, lat_lon_pairs)
    logger.info("Here after Bulk Fetch Location")
    transformed_data = []
    for row in row_group:  # Iterate through the row group (list of dictionaries)
        loc_data = location_data.get((row.get("latitude"), row.get("longitude")), {})
        transformed = {  
            "refId": row.get("refId", ""),
            "reqTime": row.get("date", ""),
            # "reqTimeConverted": datetime.fromtimestamp(row.get("date", 0) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if row.get("date") else "",
           "reqTimeConverted": datetime.fromtimestamp(float(row.get("date", 0)) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if row.get("date") else "",
            "deviceIfa": row.get("device_ifa", ""),
            "os": row.get("os", ""),
            "osv": row.get("os_version", ""),
            "normalized_osv": row.get("normalized_os_version", ""),
            "ipAddress": row.get("ip", ""),
            "carrier": row.get("carrier", ""),
            "connectionType": int(row.get("connection_type", 0)) if row.get("connection_type") else None,
            "device_vendor": row.get("device_vendor", ""),
            "device_model": row.get("device_model", ""),
            "device_height": row.get("device_height", ""),
            "device_width": row.get("device_width", ""),
            "deviceType": row.get("device_type", ""),
            "location_type": int(row.get("location_type", 0)) if row.get("location_type") else None,
            "latitude": row.get("latitude", None),
            "longitude": row.get("longitude", None),
            "appBundle": row.get("app_bundle", ""),
            "city": loc_data.get("city", row.get("city", "")),
            "region": loc_data.get("district", ""),
            "state": loc_data.get("state", row.get("region", "")),
            "device_country_name": loc_data.get("country", row.get("device_country_name", "")),
            "device_country_code": loc_data.get("countrycode", row.get("device_country_code", "")),
            "zip": loc_data.get("postcode", row.get("zip", "")),
            "ua": row.get("ua", ""),
            "ssp": row.get("ssp_endpoint_name", ""),
            "dpidsha1": row.get("dpidsha1", ""),
            "dpidmd5": row.get("dpidmd5", ""),
         }
        transformed_data.append({key: transformed.get(key, "") for key in expected_columns})
    logger.info("Here after Transformation")
    return transformed_data
