from dotenv import load_dotenv
import os

load_dotenv(override=True)

EODHD_API_TOKEN = os.getenv("EODHD_API_TOKEN")
MONGO_HOST = os.getenv("MONGO_HOST")