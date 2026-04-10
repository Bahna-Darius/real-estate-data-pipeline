from config import (
    BASE_URL, HEADERS, NUM_PAGES_TO_SCRAPE, RAW_DATA_DIR,
    RAW_CSV_PATH, RAW_JSON_PATH
)
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone
from bs4 import Tag
import pandas as pd
import requests
import datetime
import logging
import hashlib
import random
import time
import os
import re


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def get_soup(url: str, headers: Dict[str, str]) -> Optional[BeautifulSoup]:
    """
    Sends a GET request to the specified URL and returns a BeautifulSoup object.
    If the request fails, it handles the error gracefully and returns None.

    Args:
        url (str): The target URL to scrape.
        headers (Dict[str, str]): The HTTP headers to pass with the request.

    Returns:
        Optional[BeautifulSoup]: The parsed HTML content, or None if the request failed.
    """
    try:
        response = requests.get(url, headers=headers, timeout=10)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.exceptions.RequestException as e:
        logging.error(f"[-] Request failed for {url}: {e}")
        return None


def parse_listing(listing: Tag) -> Dict[str, Any]:
    """
    Extracts data from a single real estate listing HTML element.

    Generates production-ready metadata, including a unique MD5 hash ID
    based on the URL and an ISO timestamp (UTC) of the scrape time.

    Args:
        listing (Tag): A BeautifulSoup Tag representing a single property listing.

    Returns:
        Dict[str, Any]: A dictionary containing the extracted listing data,
        formatted for a Bronze-layer data lake or database schema.
    """
    # 1. Extract Link and Title
    # find("a") returns the first <a> which is often an image wrapper with no text.
    # We iterate to find the first <a> that actually has visible text content.
    title = "N/A"
    ad_url = "N/A"
    for a_tag in listing.find_all("a", href=True):
        text = a_tag.get_text(strip=True)
        if text:
            title = text
            href_value = a_tag.get('href')
            ad_url = f"https://www.storia.ro{href_value}" if href_value.startswith("/") else href_value
            break

    # 2. Generate Primary Key (MD5 hash based on the URL)
    listing_id = hashlib.md5(ad_url.encode('utf-8')).hexdigest() if ad_url != "N/A" else "N/A"

    # 3. Extract Price
    price = "N/A"
    price_element = listing.find(string=re.compile(r"€"))
    # Added defensive check for `.parent` to prevent AttributeError
    if price_element and price_element.parent:
        price = price_element.parent.get_text(strip=True).replace('\xa0', ' ')

    # 4. Extract Area (m²)
    area = "N/A"
    m2_elements = listing.find_all(string=re.compile(r"m²"))
    for el in m2_elements:
        if "€" not in el and el.parent:
            area = el.parent.get_text(strip=True)
            break

    # 5. Extract Number of Rooms from title
    # Scanning all text nodes finds label elements like "Numărul de camere" or the full title string.
    # Extracting from title is more reliable: "Apartament 3 camere..." -> "3 camere"
    rooms = "N/A"
    room_match = re.search(r"(\d+)\s*camer[aăe]", title, re.IGNORECASE)
    if room_match:
        rooms = room_match.group(0)
    elif re.search(r"garsonier[aă]", title, re.IGNORECASE):
        # A studio apartment counts as 1 room
        rooms = "1 camera"

    # 6. Extract Location / Neighborhood
    location = "N/A"
    # storia.ro uses diacritics: "București" — regex must match both forms
    location_elements = listing.find_all(string=re.compile(r"Bucure[sș]ti", re.IGNORECASE))
    if location_elements and location_elements[0].parent:
        raw_location = location_elements[0].parent.get_text(strip=True)
        # Remove "București"/"Bucuresti" and surrounding punctuation to isolate neighborhood/sector
        location = re.sub(r'(?i)Bucure[sș]ti', '', raw_location).strip(" ,-")

    # Return the complete schema object
    return {
        "listing_id": listing_id,
        "scraped_at": datetime.datetime.now().isoformat(),
        "source": "storia.ro",
        "title": title,
        "price": price,
        "area": area,
        "rooms": rooms,
        "location": location,
        "url": ad_url
    }


def save_data(scraped_data: List[Dict[str, str]], output_dir: str) -> None:
    """
    Saves the scraped data to CSV and JSON formats using an incremental load (upsert) approach
    to prevent duplicate entries based on the listing's URL.

    Args:
        scraped_data (List[Dict[str, str]]): The fresh data extracted during this run.
        output_dir (str): The directory where the raw data files should be stored.
    """
    if not scraped_data:
        logging.info("[-] No data to save.")
        return

    os.makedirs(output_dir, exist_ok=True)
    csv_filename = RAW_CSV_PATH
    json_filename = RAW_JSON_PATH

    new_df = pd.DataFrame(scraped_data)

    if os.path.exists(csv_filename):
        logging.info(f"[*] Existing database found! Performing Incremental Load...")
        existing_df = pd.read_csv(csv_filename)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # Upsert logic: keep the latest version of the listing based on the unique url
        final_df = combined_df.drop_duplicates(subset=['url'], keep='last')

        new_listings_count = len(final_df) - len(existing_df)

        if new_listings_count > 0:
            logging.info(f"[+] Added {new_listings_count} NEW listings to the database.")
        else:
            logging.info("[*] Run complete. No new unique listings found.")

    else:
        logging.info(f"[*] No existing database found. Creating first full load...")
        final_df = new_df

    final_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    final_df.to_json(json_filename, orient="records", force_ascii=False, indent=4)
    logging.info(f"[+] Success! Database now contains {len(final_df)} total records.")


def main() -> None:
    """
    The main orchestrator function. It handles pagination, coordinates the extraction
    functions, applies polite delays, and saves the final output.
    """
    scraped_data: List[Dict[str, str]] = []
    num_pages = NUM_PAGES_TO_SCRAPE

    logging.info(f"[*] Starting scraper for {num_pages} pages...")

    for page in range(1, num_pages + 1):
        url = f"{BASE_URL}?page={page}"

        logging.info(f"[*] Scraping Page {page}: {url}")

        # 1. Fetch HTML
        soup = get_soup(url, HEADERS)
        if not soup:
            logging.error(f"[-] Skipping page {page} due to fetch error.")
            continue

        listings = soup.find_all("article")
        logging.info(f"[+] Found {len(listings)} listings on page {page}.")

        if not listings:
            logging.warning("[-] No listings found on this page. Stopping pagination.")
            break

        # 2. Extract Data
        for listing in listings:
            listing_dict = parse_listing(listing)
            scraped_data.append(listing_dict)

        # 3. Polite Delay
        if page < num_pages:
            sleep_time = random.uniform(2.0, 4.0)
            logging.info(f"[*] Sleeping for {sleep_time:.2f} seconds to be polite...")
            time.sleep(sleep_time)

    # 4. Save Data
    if scraped_data:
        logging.info("--- Saving All Data ---")
        save_data(scraped_data, RAW_DATA_DIR)
    else:
        logging.warning("[-] No data was scraped at all.")


if __name__ == "__main__":
    main()