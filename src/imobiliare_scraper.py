from bs4 import BeautifulSoup
import pandas as pd
import requests
import re


def test_scraper():
    url = "https://www.storia.ro/ro/rezultate/vanzare/apartament/bucuresti"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"[*] Attempting to access: {url}")

    response = requests.get(
        url, headers=headers
    )
    print(f"[*] Status Code: {response.status_code}\n")

    if response.status_code == 200:
        print("[+] Success! Connected to the website.")
        soup = BeautifulSoup(response.text, 'html.parser')

        listings = soup.find_all("article")
        print(f"[*] Found {len(listings)} listings on this page!")

        if len(listings) > 0:
            print("\n--- Extracting ALL Prices ---")

            scraped_data = []

            for index, listing in enumerate(listings, start=1):
                # Extract Price
                price_element = listing.find(string=re.compile("€"))
                price = price_element.parent.get_text(strip=True).replace('\xa0', '') if price_element else "N/A"
                # Extract Area
                area = "N/A"
                m2_elements = listing.find_all(string=re.compile("m²"))

                for el in m2_elements:
                    if "€" not in el:
                        area = el.parent.get_text(strip=True)
                        break


                # Extract the Title and Link
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

                listing_dict = {
                    "Title": title,
                    "Price": price,
                    "Area": area,
                    "Link": ad_url
                }
                scraped_data.append(listing_dict)

            # Transform list data in DF:
            df = pd.DataFrame(scraped_data)
            csv_filename = "storia_raw_data.csv"
            json_filename = "storia_raw_data.json"

            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            df.to_json(json_filename, orient="records", force_ascii=False, indent=4)

            print(f"\n[+] Successfully saved {len(df)} records to {csv_filename} and {json_filename}!")


    else:
        print("[-] Still 0 listings. We might need to inspect the page structure further.")



if __name__ == "__main__":
    test_scraper()