import redis
import os
import json
from logger import get_logger  
from config import config

# Get Redis configuration from environment variables
# REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
# REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
# REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_HOST = config.REDIS_HOST
REDIS_PORT = config.REDIS_PORT
REDIS_DB = config.REDIS_DB

# Load the list from the environment variable
DEFAULT_PREDEFINED_VENDORS = os.getenv('DEFAULT_PREDEFINED_VENDORS', '').split(',')

# Initialize logger
logger = get_logger("RedisProcess")

logger.info("Connecting to Redis at {}:{}".format(REDIS_HOST, REDIS_PORT))

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()
    logger.info("Successfully connected to Redis")
except redis.ConnectionError as e:
    logger.error("Failed to connect to Redis: %s", str(e))
    raise

def load_or_create_vendors():
    logger.info("Loading vendor list from Redis")
    vendors = r.smembers('vendors')
    if not vendors:
        logger.warning("No vendors found, initializing with default vendors")
        r.sadd('vendors', *DEFAULT_PREDEFINED_VENDORS)
        logger.info("Default vendors added: %s", DEFAULT_PREDEFINED_VENDORS)
        return set(DEFAULT_PREDEFINED_VENDORS)
    # logger.info("Loaded vendors: %s", vendors)
    return set(vendors)

def save_vendor(vendor):
    logger.info("Saving vendor: %s", vendor)
    r.sadd('vendors', vendor)
    logger.info("Vendor %s saved successfully", vendor)

def get_model_mapping():
    logger.info("Fetching model mapping from Redis")
    mapping = r.hgetall('model_mapping')
    result = {key: json.loads(value) for key, value in mapping.items()}
    # logger.info("Model mapping fetched: %s", result)
    return result

def update_model_mapping(model, details):
    model = model.strip().lower()
    logger.info("Updating model mapping for: %s", model)
    r.hset('model_mapping', model.lower(), json.dumps(details))
    logger.info("Model %s updated with details: %s", model, details)
