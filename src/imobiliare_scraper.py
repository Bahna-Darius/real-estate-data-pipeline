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
import json
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


_ROOMS_TEXT_MAP = {
    "ONE": 1, "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5,
    "SIX": 6, "SEVEN": 7, "EIGHT": 8, "NINE": 9, "TEN": 10,
}


def extract_nextdata_rooms(soup: BeautifulSoup) -> Dict[str, int]:
    """
    Parses __NEXT_DATA__ and returns slug-based URL → numberOfRooms.
    Works on all paginated pages (unlike JSON-LD which only appears on page 1).
    """
    url_to_rooms: Dict[str, int] = {}
    nd_script = soup.find("script", id="__NEXT_DATA__")
    if not nd_script or not nd_script.string:
        return url_to_rooms
    try:
        data = json.loads(nd_script.string)
        items = data["props"]["pageProps"]["data"]["searchAds"]["items"]
        for item in items:
            slug = item.get("slug")
            rooms_text = item.get("roomsNumber", "")
            rooms_num = _ROOMS_TEXT_MAP.get(rooms_text)
            if slug and rooms_num is not None:
                full_url = f"https://www.storia.ro/ro/oferta/{slug}"
                url_to_rooms[full_url] = rooms_num
    except (KeyError, TypeError, json.JSONDecodeError):
        pass
    return url_to_rooms


def parse_listing(listing: Tag, rooms_lookup: Dict[str, int] = None) -> Dict[str, Any]:
    """
    Extracts data from a single real estate listing HTML element.

    Generates production-ready metadata, including a unique MD5 hash ID
    based on the URL and an ISO timestamp (UTC) of the scrape time.

    Args:
        listing (Tag): A BeautifulSoup Tag representing a single property listing.
        rooms_lookup (Dict[str, int]): URL → numberOfRooms map from page JSON-LD.

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

    # 5. Extract Number of Rooms
    # Patterns that cover: "3 camere", "3 Camere", "3-camere", "3 cam.", "3 camare" (typo)
    ROOM_PATTERN = re.compile(r"(\d+)[-\s]*cam[ae]r[eaă]?", re.IGNORECASE)
    CAM_ABBREV   = re.compile(r"(\d+)[-\s]*cam[.\s]",        re.IGNORECASE)
    STUDIO_PATTERN = re.compile(r"gars[io]{1,2}ner[aă]",     re.IGNORECASE)
    # Matches "Numărul de camere : 1" or "Nr. camere: 2" in concatenated get_text() output
    CAMERE_LABEL_PATTERN = re.compile(
        r"num[aă]r(?:ul)?\s+de\s+camere\s*:?\s*(\d+)", re.IGNORECASE
    )

    def _extract_rooms(text: str):
        text = text.replace("-", " ")
        m = CAMERE_LABEL_PATTERN.search(text)
        if m:
            return f"{m.group(1)} camere"
        m = ROOM_PATTERN.search(text)
        if m:
            return f"{m.group(1)} camere"
        m = CAM_ABBREV.search(text)
        if m:
            return f"{m.group(1)} camere"
        if STUDIO_PATTERN.search(text):
            return "1 camera"
        return None

    def _extract_rooms_from_label(tag: Tag) -> Optional[str]:
        # Targets React-rendered: <div>Numărul de camere<!-- -->:</div><div>3 </div>
        label = tag.find(string=re.compile(r"num[aă]r(?:ul)?\s+de\s+camere", re.IGNORECASE))
        if label and label.parent:
            # value may be the next sibling of the label's parent div
            value_el = label.parent.find_next_sibling()
            if value_el:
                val = value_el.get_text(strip=True)
                if val.isdigit():
                    return f"{val} camere"
            # fallback: value may be in the grandparent's next sibling
            if label.parent.parent:
                value_el = label.parent.parent.find_next_sibling()
                if value_el:
                    val = value_el.get_text(strip=True)
                    if val.isdigit():
                        return f"{val} camere"
        return None

    # JSON-LD lookup is the most reliable source — checked first
    jsonld_rooms = None
    if rooms_lookup and ad_url in rooms_lookup:
        jsonld_rooms = f"{rooms_lookup[ad_url]} camere"

    rooms = (
        jsonld_rooms
        or _extract_rooms(title)
        or _extract_rooms(ad_url)
        or _extract_rooms_from_label(listing)
        or _extract_rooms(listing.get_text(" ", strip=True))
        or "N/A"
    )

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

        # 2. Build __NEXT_DATA__ rooms lookup for this page, then extract per listing
        rooms_lookup = extract_nextdata_rooms(soup)
        logging.info(f"[*] __NEXT_DATA__ rooms lookup: {len(rooms_lookup)} entries found.")

        for listing in listings:
            listing_dict = parse_listing(listing, rooms_lookup)
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