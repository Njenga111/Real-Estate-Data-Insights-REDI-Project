# etl/monitor/logger.py

import logging
from config.config import LOG_FILE

def setup_logger():
    """Sets up the logger to log to both file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )

def log_message(message):
    """Logs a message to both console and log file."""
    logging.info(message)
