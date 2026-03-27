from bs4 import BeautifulSoup
import pandas as pd
import requests
import random
import time
import re
import os


def test_scraper():
    base_url = "https://www.storia.ro/ro/rezultate/vanzare/apartament/bucuresti"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # 1. Initialize the data storage outside the page loop
    # We want to collect data from all pages into the same list.
    scraped_data = []

    # Set the number of pages to extract (testing with 3 pages initially)
    num_pages = 3

    print(f"[*] Starting scraper for {num_pages} pages...")

    # 2. Main loop iterating through pages
    for page in range(1, num_pages + 1):

        # Build the dynamic link for the current page
        url = f"{base_url}?page={page}"
        print(f"\n[*] Scraping Page {page}: {url}")

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            listings = soup.find_all("article")
            print(f"[+] Found {len(listings)} listings on page {page}.")

            if len(listings) > 0:
                # Inner loop to extract data from each apartment listing
                for listing in listings:
                    # Extract Price
                    price_element = listing.find(string=re.compile("€"))
                    price = price_element.parent.get_text(strip=True).replace('\xa0', ' ') if price_element else "N/A"

                    # Extract Area
                    area = "N/A"
                    m2_elements = listing.find_all(string=re.compile("m²"))
                    for el in m2_elements:
                        if "€" not in el:
                            area = el.parent.get_text(strip=True)
                            break

                    # Extract Title and Link
                    title = "N/A"
                    ad_url = "N/A"
                    all_links = listing.find_all("a", href=True)

                    for link in all_links:
                        href_value = link.get('href')
                        text_inside_link = link.get_text(strip=True)

                        if href_value and text_inside_link:
                            title = text_inside_link
                            ad_url = href_value
                            if ad_url is not None and ad_url.startswith("/"):
                                ad_url = "https://www.storia.ro" + ad_url
                            break

                    # Append data to the main list
                    listing_dict = {
                        "Title": title,
                        "Price": price,
                        "Area": area,
                        "Link": ad_url
                    }
                    scraped_data.append(listing_dict)
            else:
                print("[-] No listings found on this page. Structure might have changed or page is empty.")

        else:
            print(f"[-] Error accessing page {page}. Status code: {response.status_code}")
            break  # Stop extraction if an error occurs (e.g., being blocked)

        # 3. HUMAN-LIKE PAUSE (MANDATORY) 🛌
        # Pause the script for 2 to 4 seconds before requesting the next page
        sleep_time = random.uniform(2.0, 4.0)
        print(f"[*] Sleeping for {sleep_time:.2f} seconds to be polite...")
        time.sleep(sleep_time)

    # --- DATA SAVING SECTION (Executes after all pages are processed) ---
    if len(scraped_data) > 0:
        print("\n--- Saving All Data ---")
        df = pd.DataFrame(scraped_data)

        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        output_dir = os.path.join(project_root, "data", "raw")
        os.makedirs(output_dir, exist_ok=True)

        csv_filename = os.path.join(output_dir, "storia_raw_data.csv")
        json_filename = os.path.join(output_dir, "storia_raw_data.json")

        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        df.to_json(json_filename, orient="records", force_ascii=False, indent=4)

        print(f"[+] Successfully saved {len(df)} total records to data/raw/")
    else:
        print("[-] No data was scraped.")


if __name__ == "__main__":
    test_scraper()