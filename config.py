import os
from dotenv import load_dotenv  # type: ignore
from typing import List, Dict
from logger import get_logger

load_dotenv()

logger = get_logger(__name__)

class EnvironmentVariableError(Exception):
    pass

class Config:
    # # Development mode
    # DEV_MODE = os.getenv('DEVELOPMENT_MODE', 'true').lower() == 'true'

    # # Linode S3 Configuration
    # ACCESS_KEY_ID = os.getenv('DESTINATION_LINODE_DEV_ACCESS_KEY_ID') if DEV_MODE else os.getenv('DESTINATION_LINODE_PROD_ACCESS_KEY_ID')
    # SECRET_ACCESS_KEY = os.getenv('DESTINATION_LINODE_DEV_SECRET_ACCESS_KEY') if DEV_MODE else os.getenv('DESTINATION_LINODE_PROD_SECRET_ACCESS_KEY')
    # ENDPOINT_URL = os.getenv('DESTINATION_DEV_ENDPOINT_URL') if DEV_MODE else os.getenv('DESTINATION_PROD_ENDPOINT_URL')
    # REGION_NAME = os.getenv('DESTINATION_DEV_REGION_NAME') if DEV_MODE else os.getenv('DESTINATION_PROD_REGION_NAME')
    # BUCKET_NAME = os.getenv('DESTINATION_BUCKET_DEV') if DEV_MODE else os.getenv('DESTINATION_BUCKET_PROD')

    DEV_MODE = os.getenv('DEVELOPMENT_MODE', 'true').lower() == 'true'
    ACCESS_KEY_TEST = os.getenv('ACCESS_KEY_TEST') if DEV_MODE else os.getenv('ACCESS_KEY_TEST')
    SECRET_KEY_TEST = os.getenv('SECRET_KEY_TEST') if DEV_MODE else os.getenv('SECRET_KEY_TEST')
    ENDPOINT_URL_TEST = os.getenv('ENDPOINT_URL_TEST') if DEV_MODE else os.getenv('ENDPOINT_URL_TEST')
    REGION_NAME_TEST = os.getenv('REGION_NAME_TEST') if DEV_MODE else os.getenv('REGION_NAME_TEST')
    BUCKET_NAME_TEST = os.getenv('BUCKET_NAME_TEST') if DEV_MODE else os.getenv('BUCKET_NAME_TEST')
    REVERSE_GEOCODER_API=os.getenv('REVERSE_GEOCODER_API')
    # LOG_INFO = os.getenv('LOG_INFO', 'false').lower() == 'true'
    REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))

    @classmethod
    def check_env_variables(cls) -> Dict[str, str]:
        missing_vars: List[str] = []
        for var_name, var_value in cls.__dict__.items():
            if not var_name.startswith('__') and not callable(var_value) and var_value is None:
                missing_vars.append(var_name)

        if missing_vars:
            error_message = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_message)
            raise EnvironmentVariableError(error_message)

        logger.info("All environment variables are set.")
        return {var_name: var_value for var_name, var_value in cls.__dict__.items()
                if not var_name.startswith('__') and not callable(var_value)}


def validate_config():
    try:
        return Config.check_env_variables()
    except EnvironmentVariableError as e:
        logger.error(f"Configuration error: {str(e)}")
        return None

config = Config()
