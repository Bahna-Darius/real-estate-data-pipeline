import os


BASE_URL = "https://www.storia.ro/ro/rezultate/vanzare/apartament/bucuresti"
NUM_PAGES_TO_SCRAPE = 100


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")


RAW_CSV_PATH = os.path.join(RAW_DATA_DIR, "storia_raw_data.csv")
RAW_JSON_PATH = os.path.join(RAW_DATA_DIR, "storia_raw_data.json")

# Azure
AZURE_CONTAINER_NAME = "bronze"
BLOB_NAME = "storia_raw_data.json"
