# 🏠 Romanian Real Estate Data Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/PySpark-Databricks-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Azure_Blob_Storage-Data_Lake-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white"/>
  <img src="https://img.shields.io/badge/Delta_Lake-Silver_Layer-003366?style=for-the-badge&logo=delta&logoColor=white"/>
  <img src="https://img.shields.io/badge/Architecture-Medallion-gold?style=for-the-badge"/>
  <img src="https://img.shields.io/badge/Status-Active-success?style=for-the-badge"/>
</p>

<p align="center">
  An end-to-end data engineering pipeline that scrapes <strong>3,700+ real estate listings</strong> from the Romanian property market (Bucharest), processes them through a <strong>Medallion Architecture</strong> (Bronze → Silver → Gold), and lands analytics-ready data in <strong>Azure Blob Storage</strong> via <strong>Databricks</strong>.
</p>

---

## 📐 Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Data Pipeline Flow                                  │
└──────────────────────────────────────────────────────────────────────────────┘

  [storia.ro]
      │
      │  HTTP GET · BeautifulSoup · __NEXT_DATA__ parsing
      ▼
┌─────────────────────────────────┐
│         🥉 BRONZE LAYER         │         Local landing zone
│                                 │  ──────────────────────────
│  · Paginated scraping (100 pg)  │  data/raw/storia_raw.json
│  · MD5 deterministic PK         │  data/raw/storia_raw.csv
│  · Incremental upsert (URL key) │
│  · 3-level rooms fallback       │
│  · UTC timestamp per record     │
└────────────────┬────────────────┘
                 │
                 │  azure_uploader.py
                 ▼
      ┌──────────────────────┐
      │  Azure Blob Storage  │   Container: bronze/
      │  storia_raw.json     │
      └──────────┬───────────┘
                 │
                 │  Databricks · PySpark · 01_Bronze_to_Silver.ipynb
                 ▼
┌─────────────────────────────────┐
│         🥈 SILVER LAYER         │         Cleaned & typed
│                                 │  ──────────────────────────
│  · Price → INT (€)              │  Azure Blob Storage
│  · Area → DOUBLE (m²)           │  Container: silver/
│  · Price/m² computed metric     │  Format: Delta Lake
│  · Rooms → INT                  │
│  · Sector extracted (regex)     │
│  · Neighborhood cleaned         │
│  · Null handling (keep rows)    │
└────────────────┬────────────────┘
                 │
                 │  Databricks · SQL · 02_Silver_to_Gold.ipynb
                 ▼
┌─────────────────────────────────┐
│         🥇 GOLD LAYER           │         Business-ready
│                                 │  ──────────────────────────
│  · Avg price by sector          │  Databricks Table
│  · Avg €/m² by neighborhood     │  default.gold_top_cartiere
│  · Listing count aggregations   │
│  · Avg rooms per zone           │
│  · Filtered: ≥ 2 listings/zone  │
└─────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool / Library | Purpose |
|-------|---------------|---------|
| **Ingestion** | `requests` + `BeautifulSoup4` | HTTP scraping with polite random delays |
| **Parsing** | `__NEXT_DATA__` JSON + HTML fallback | Robust room & metadata extraction |
| **Local Storage** | `pandas` → CSV + JSON | Bronze-layer landing & upsert logic |
| **Cloud Storage** | Azure Blob Storage | Data lake for Bronze and Silver layers |
| **Transformation** | PySpark (Databricks) | Silver-layer cleaning, typing, enrichment |
| **Serving** | Delta Lake + Databricks SQL | Gold-layer aggregations, permanent tables |
| **Config & Secrets** | `python-dotenv` | Environment variable management |
| **Language** | Python 3.12 | Pipeline orchestration |

---

## 📊 Dataset Snapshot

> Data sourced from [storia.ro](https://www.storia.ro) — Bucharest apartment listings (sale market).

| Metric | Value |
|--------|-------|
| **Total records** | 3,700+ listings |
| **Source pages scraped** | 100 pages |
| **Average asking price** | ~146,000 € |
| **Price range** | 19,000 € – 4,500,000 € |
| **Most common listing** | 2-room apartments (47%) |
| **Data freshness** | UTC timestamp per record |
| **Null rooms** | < 0.1% (2 records) |

---

## 🔁 Pipeline Stages

### 🥉 Bronze — Raw Ingestion (`imobiliare_scraper.py`)

The scraper targets apartment listings in Bucharest and handles all real-world messiness of a Next.js-rendered website:

- **Pagination** across 100 pages with polite random delays (2–4s)
- **`__NEXT_DATA__`** parsing for `roomsNumber` — the only reliable source across all pages
- **3-level fallback** for rooms: `__NEXT_DATA__` → title regex → full-text regex
- **Bulletproof location targeting**: positive match on `"bucure"` / `"ilfov"` strings, then aggressive negative filtering to eliminate UI noise (prices, timestamps, agency names)
- **Deterministic MD5 primary key** generated from the listing URL — stateless and reproducible
- **Incremental upsert**: re-runs deduplicate on URL, appending only new listings
- **Pandas `keep_default_na=False`** fix — prevents `"N/A"` strings from becoming `NaN` silently

**Output schema:**

| Field | Type | Description |
|-------|------|-------------|
| `listing_id` | `string` | MD5 hash of URL (primary key) |
| `scraped_at` | `ISO 8601` | UTC timestamp of scrape |
| `source` | `string` | `storia.ro` |
| `title` | `string` | Full listing title |
| `price` | `string` | Asking price in EUR (raw) |
| `area` | `string` | Property area in m² (raw) |
| `rooms` | `string` | Number of rooms (raw) |
| `location` | `string` | Neighborhood / sector (raw) |
| `url` | `string` | Direct link to listing |

---

### 🥈 Silver — Cleaned & Typed (`01_Bronze_to_Silver.ipynb`)

PySpark notebook on Databricks that transforms raw strings into analytics-ready types:

- `price` → `Price_EUR (INT)` via `regexp_replace` + `try_cast` (graceful on malformed data)
- `area` → `Area_sqm (DOUBLE)` via `regexp_extract`
- `Price_per_sqm (DOUBLE)` — computed metric, rounded to 2 decimals
- `rooms` → `rooms (INT)` via `regexp_extract`
- `City_Sector` — extracted with regex `sector(?:ul)?\s*[1-6]`, null-safe
- `Neighborhood` — sector and city name stripped, trimmed cleanly
- **Architectural decision:** null rows are *retained* in Silver — dropping them would corrupt Gold-layer totals (e.g. "total market value"). Null handling is delegated to Gold queries via `COALESCE` / `WHERE`.
- Saved as **Delta Lake** format (versioned, ACID, time-travel ready)

---

### 🥇 Gold — Business Aggregations (`02_Silver_to_Gold.ipynb`)

Pure SQL layer (dbt-style) that produces the final reporting table:

```sql
CREATE OR REPLACE TABLE default.gold_top_cartiere AS
SELECT
    COALESCE(City_Sector, 'Ilfov / Necunoscut') AS Sector,
    Neighborhood                                  AS Cartier,
    COUNT(listing_id)                             AS Numar_Anunturi,
    ROUND(AVG(Price_EUR), 0)                      AS Pret_Mediu_Total_EUR,
    ROUND(AVG(Price_per_sqm), 0)                  AS Pret_Mediu_MP_EUR,
    ROUND(AVG(rooms), 1)                          AS Numar_Mediu_Camere
FROM delta.`/Volumes/workspace/default/raw_data/silver_storia/`
WHERE Neighborhood IS NOT NULL AND Neighborhood != ''
GROUP BY City_Sector, Neighborhood
HAVING COUNT(listing_id) >= 2
ORDER BY Pret_Mediu_MP_EUR DESC
```

---

## 📁 Project Structure

```
real-estate-data-pipeline/
├── src/
│   ├── imobiliare_scraper.py       # Bronze: scrape, parse & local upsert
│   ├── azure_uploader.py           # Cloud: upload Bronze data to Azure Blob
│   ├── config.py                   # Centralized config (URLs, paths, Azure)
│   └── utils/
│       └── fix_null_rooms.py       # One-off: retroactive rooms fix via detail pages
├── databricks_notebooks/
│   ├── 01_Bronze_to_Silver.ipynb   # PySpark: clean, type-cast, enrich → Delta
│   └── 02_Silver_to_Gold.ipynb     # SQL: aggregate → gold_top_cartiere table
├── data/
│   └── raw/                        # Local Bronze landing zone (gitignored)
│       ├── storia_raw_data.csv
│       └── storia_raw_data.json
├── .env                            # Azure credentials (gitignored)
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- Azure Storage account (for cloud upload)
- Databricks workspace (for Silver & Gold layers)

### 1. Clone & install

```bash
git clone https://github.com/Bahna-Darius/real-estate-data-pipeline.git
cd real-estate-data-pipeline
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file in the project root:

```env
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT;AccountKey=YOUR_KEY;...
```

### 3. Run the pipeline

```bash
# Step 1 — Scrape & save Bronze layer locally
python src/imobiliare_scraper.py

# Step 2 — Upload Bronze data to Azure Blob Storage
python src/azure_uploader.py
```

### 4. Databricks (Silver & Gold)

Upload `databricks_notebooks/` to your Databricks workspace and run in order:
1. `01_Bronze_to_Silver.ipynb` — reads from Azure, writes Delta to Silver
2. `02_Silver_to_Gold.ipynb` — reads Silver, creates `gold_top_cartiere` table

---

## 🧠 Key Engineering Decisions

**Incremental load over full refresh**
URL-based deduplication ensures repeated runs only append genuinely new listings — no wasted API calls, no duplicate records, lower storage cost.

**`__NEXT_DATA__` as primary rooms source**
storia.ro renders via Next.js; room count is only reliable in the embedded JSON blob (`__NEXT_DATA__`), not in the rendered HTML. JSON-LD `numberOfRooms` only appears on page 1. The 3-level fallback (Next data → title regex → full-text regex) covers all edge cases.

**Deterministic MD5 primary keys**
Hashing the listing URL produces stable, reproducible IDs without a database sequence — making the pipeline stateless, testable, and cloud-portable.

**Nulls retained in Silver**
Dropping null rows at the Silver layer would silently corrupt Gold aggregations (e.g., average price calculations would exclude valid listings that simply have no sector data). Null handling is a Gold-layer concern, applied per query via `COALESCE`.

**`keep_default_na=False` in pandas**
Without this flag, pandas silently converts the string `"N/A"` to `NaN` on CSV read — causing downstream type errors and false null counts. Explicitly disabled to preserve data intent.

---

## 🗺️ Roadmap

- [x] Bronze layer scraper with incremental upsert
- [x] Azure Blob Storage integration
- [x] Silver layer — numeric typing, null cleanup, sector/neighborhood extraction
- [x] Gold layer — aggregated market analytics (SQL, dbt-style)
- [x] Robust rooms extraction with 3-level fallback
- [x] Retroactive null-rooms fixer (`fix_null_rooms.py`)
- [ ] 🐳 Docker + Docker Compose for reproducible local execution
- [ ] 📊 Streamlit dashboard — price trends, sector heatmap, room distribution
- [ ] 🧪 Unit tests with `pytest` for scraper and uploader
- [ ] ⚙️ GitHub Actions CI/CD — automated test runs on push
- [ ] 🌀 Apache Airflow DAG for scheduled pipeline orchestration
