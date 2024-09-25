from dotenv import load_dotenv
import os

load_dotenv(override=True)

EODHD_REAL_TOKEN = os.getenv("EODHD_REAL_TOKEN")
EODHD_DEMO_TOKEN = os.getenv("EODHD_DEMO_TOKEN")
MONGO_HOST = os.getenv("MONGO_HOST")
