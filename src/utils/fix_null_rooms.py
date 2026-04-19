"""
Retroactive fix for listings with rooms=null.
For each null-rooms record:
  - Fetches the detail page
  - Extracts roomsNumber from __NEXT_DATA__
  - If 410 Gone (expired listing) → removes from DB
  - Saves updated JSON + CSV
"""
import os, sys, json, time, random, logging
import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(__file__))
from ..config import HEADERS, RAW_JSON_PATH, RAW_CSV_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

_ROOMS_TEXT_MAP = {
    "ONE": 1, "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5,
    "SIX": 6, "SEVEN": 7, "EIGHT": 8, "NINE": 9, "TEN": 10,
}


def fetch_rooms_from_detail(url: str) -> tuple[str | None, str]:
    """
    Returns (rooms_value, status) where status is 'fixed', 'expired', or 'failed'.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 410:
            return None, "expired"
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # 1. Try __NEXT_DATA__ (most reliable)
        nd = soup.find("script", id="__NEXT_DATA__")
        if nd and nd.string:
            try:
                data = json.loads(nd.string)
                ad = data["props"]["pageProps"]["ad"]
                rooms_text = ad.get("roomsNumber")
                if rooms_text and rooms_text in _ROOMS_TEXT_MAP:
                    return f"{_ROOMS_TEXT_MAP[rooms_text]} camere", "fixed"
                # characteristics is a list of {key, value} dicts
                for char in ad.get("characteristics", []) or []:
                    if isinstance(char, dict) and char.get("key") == "rooms_num":
                        val = char.get("value")
                        if val and str(val).isdigit():
                            return f"{val} camere", "fixed"
            except (KeyError, TypeError):
                pass

        # 2. Fallback: JSON-LD numberOfRooms
        ld = soup.find("script", type="application/ld+json")
        if ld and ld.string:
            try:
                data = json.loads(ld.string)
                graph = data.get("@graph", [])
                for node in graph:
                    n = node.get("numberOfRooms")
                    if n is not None:
                        return f"{int(n)} camere", "fixed"
                    for pv in node.get("additionalProperty", []):
                        if "camere" in pv.get("name", "").lower():
                            val = pv.get("value", "").strip()
                            if val.isdigit():
                                return f"{val} camere", "fixed"
            except (KeyError, TypeError, ValueError):
                pass

        return None, "failed"

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 410:
            return None, "expired"
        return None, "failed"
    except requests.exceptions.RequestException:
        return None, "failed"


def main():
    with open(RAW_JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)

    null_records = [d for d in data if d.get("rooms") in (None, "N/A")]
    logging.info(f"[*] Found {len(null_records)} null-rooms records to fix.")

    fixed = expired = failed = 0

    for i, record in enumerate(null_records, 1):
        url = record.get("url", "")
        logging.info(f"[{i}/{len(null_records)}] {url[-70:]}")

        rooms, status = fetch_rooms_from_detail(url)

        if status == "fixed":
            record["rooms"] = rooms
            fixed += 1
            logging.info(f"  -> FIXED: {rooms}")
        elif status == "expired":
            record["_delete"] = True
            expired += 1
            logging.info(f"  -> EXPIRED (410), will remove")
        else:
            failed += 1
            logging.info(f"  -> FAILED, keeping as-is")

        time.sleep(random.uniform(1.0, 2.0))

    # Remove expired listings
    data = [d for d in data if not d.get("_delete")]

    # Save
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    df = pd.DataFrame(data)
    df.to_csv(RAW_CSV_PATH, index=False, encoding="utf-8-sig")

    logging.info(f"\n[+] Done. Fixed: {fixed} | Expired (removed): {expired} | Failed: {failed}")
    logging.info(f"[+] Database now contains {len(data)} records.")


if __name__ == "__main__":
    main()
