# config/config.py

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Log file path
LOG_FILE = "logs/etl.log"
