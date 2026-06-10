import os

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

CITY_NAME = "bucuresti"
BASE_URL = f"https://www.storia.ro/ro/rezultate/vanzare/apartament/{CITY_NAME}"
NUM_PAGES_TO_SCRAPE = 100

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

RAW_CSV_PATH = os.path.join(RAW_DATA_DIR, "storia_raw_data.csv")
RAW_JSON_PATH = os.path.join(RAW_DATA_DIR, "storia_raw_data.json")

OUTPUT_DIR_SILVER = os.path.join(PROJECT_ROOT, "data", "silver_storia")
OUTPUT_DIR_GOLD   = os.path.join(PROJECT_ROOT, "data", "gold")

# ---------------------------------------------------------------------------
# Azure
# ---------------------------------------------------------------------------

AZURE_BRONZE_CONTAINER_NAME    = "bronze-real-estate"
BLOB_NAME                   = "storia_raw_data.json"

AZURE_SILVER_CONTAINER_NAME = "silver-real-estate"

AZURE_GOLD_CONTAINER_NAME   = "gold-real-estate"

