
# import logging
# import os
# from dotenv import load_dotenv  # type: ignore

# load_dotenv()

# LOG_INFO = os.getenv('LOG_INFO', 'false').lower() == 'true'

# def get_logger(name: str) -> logging.Logger:
#     logger = logging.getLogger(name)
#     logger.setLevel(logging.INFO if LOG_INFO else logging.ERROR)
    
#     if logger.hasHandlers():
#         logger.handlers.clear()

#     console_handler = logging.StreamHandler()
#     console_handler.setLevel(logging.INFO if LOG_INFO else logging.ERROR)

#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     console_handler.setFormatter(formatter)

#     logger.addHandler(console_handler)
#     return logger

import logging
import os
from dotenv import load_dotenv  # type: ignore

load_dotenv()

LOG_INFO = os.getenv('LOG_INFO', 'false').lower() == 'true'
LOG_FILE = os.getenv('LOG_FILE', 'pipeline.log')  # Default log file name

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO if LOG_INFO else logging.ERROR)

    # Clear existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO if LOG_INFO else logging.ERROR)

    # Create a file handler
    file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO if LOG_INFO else logging.ERROR)

    # Define log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

# Example usage
logger = get_logger("Data Pipeline")
logger.info("This is an info log.")
logger.error("This is an error log.")
