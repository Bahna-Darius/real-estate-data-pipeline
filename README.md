# Romanian Real Estate Data Pipeline

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure_Blob_Storage-Data_Lake-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data_Cleaning-150458?style=flat&logo=pandas&logoColor=white)
![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup4-Web_Scraping-brightgreen?style=flat)
![Status](https://img.shields.io/badge/Status-Active-success?style=flat)

An end-to-end data engineering pipeline that scrapes real estate listings from the Romanian property market, applies a **Medallion Architecture** (Bronze → Silver) for data quality, and lands clean data in **Azure Blob Storage** — ready for downstream analytics.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Pipeline Flow                           │
└─────────────────────────────────────────────────────────────────────┘

  [storia.ro]                                      [Azure Blob Storage]
      │                                                      │
      │  HTTP GET (requests + BeautifulSoup)                 │
      ▼                                                      │
┌──────────────┐    ┌─────────────────────┐    ┌────────────▼──────────┐
│   SCRAPER    │───▶│   BRONZE LAYER      │───▶│  Azure Container:     │
│              │    │   Raw JSONL / CSV   │    │  "bronze"             │
│ - Pagination │    │   storia_raw.json   │    │  storia_raw.json      │
│ - Upsert     │    │   storia_raw.csv    │    └───────────────────────┘
│ - MD5 PK     │    └─────────┬───────────┘
└──────────────┘              │
                              │  Numeric parsing, null cleanup
                              ▼
                   ┌─────────────────────┐    ┌───────────────────────┐
                   │   SILVER LAYER      │───▶│  Azure Container:     │
                   │   Cleaned Parquet   │    │  "silver"             │
                   │   Typed Schema      │    │  storia_clean.parquet │
                   └─────────────────────┘    └───────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | `requests` + `BeautifulSoup4` | HTTP scraping with polite delays |
| Storage (local) | CSV + JSONL | Bronze-layer raw data |
| Cloud Storage | Azure Blob Storage | Data lake landing zone |
| Transformation | `pandas` | Silver-layer cleaning & type casting |
| Config | `python-dotenv` | Secrets & environment management |
| Language | Python 3.12 | Pipeline orchestration |

---

## Pipeline Stages

### Bronze Layer — Raw Ingestion
- Scrapes apartment listings from [storia.ro](https://www.storia.ro) (Bucharest market)
- Extracts: `title`, `price`, `area (m²)`, `rooms`, `location`, `url`
- Generates a **deterministic MD5 primary key** per listing (based on URL)
- Performs **incremental upsert** — re-runs never create duplicates
- Uploads raw JSONL to **Azure Blob Storage** (`bronze` container)

### Silver Layer — Cleaned & Typed
- Strips currency symbols, normalizes `price` and `area` to numeric types
- Standardizes `rooms` field with 3-level fallback extraction (title → full text → HTML attributes)
- Removes null-heavy or malformed records
- Uploads clean data to **Azure Blob Storage** (`silver` container)

---

## Project Structure

```
real-estate-data-pipeline/
├── src/
│   ├── imobiliare_scraper.py   # Bronze: scrape & ingest
│   ├── azure_uploader.py       # Cloud: upload to Azure Blob
│   └── config.py               # Centralized configuration
├── data/
│   └── raw/                    # Local Bronze landing zone
│       ├── storia_raw.csv
│       └── storia_raw.json
├── .env                        # Azure credentials (not committed)
├── requirements.txt
└── README.md
```

---

## Getting Started

### 1. Clone & install dependencies

```bash
git clone https://github.com/Bahna-Darius/real-estate-data-pipeline.git
cd real-estate-data-pipeline
pip install -r requirements.txt
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```env
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
```

### 3. Run the pipeline

```bash
# Step 1 — Scrape & save Bronze layer locally
python src/imobiliare_scraper.py

# Step 2 — Upload Bronze data to Azure
python src/azure_uploader.py
```

---

## Data Schema

| Field | Type | Description |
|-------|------|-------------|
| `listing_id` | `string` | MD5 hash of listing URL (primary key) |
| `scraped_at` | `ISO 8601` | UTC timestamp of scrape |
| `source` | `string` | Origin website (`storia.ro`) |
| `title` | `string` | Full listing title |
| `price` | `string` | Asking price in EUR |
| `area` | `string` | Property area in m² |
| `rooms` | `string` | Number of rooms |
| `location` | `string` | Neighborhood / sector in Bucharest |
| `url` | `string` | Direct link to listing |

---

## Roadmap

- [x] Bronze layer scraper with incremental upsert
- [x] Azure Blob Storage integration
- [x] Silver layer transformation (numeric typing, null cleanup)
- [x] Robust rooms extraction with 3-level fallback
- [ ] Apache Airflow DAG for scheduled orchestration
- [ ] Docker Compose setup for reproducible execution
- [ ] Gold layer with aggregated market analytics
- [ ] Power BI / Grafana dashboard for price trends

---

## Key Engineering Decisions

**Incremental load over full refresh** — The pipeline uses URL-based deduplication so repeated runs only append new listings, avoiding redundant API calls and keeping storage costs low.

**Medallion Architecture** — Separating raw (Bronze) from clean (Silver) data means upstream failures never corrupt analytics-ready datasets.

**Deterministic primary keys** — MD5 hashes of listing URLs generate stable, reproducible IDs without needing a database sequence, making the pipeline stateless and cloud-portable.
