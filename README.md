# Kedra WRC Legal Document Scraper

A scalable scraping pipeline that extracts legal decisions and determinations from Ireland's [Workplace Relations Commission](https://www.workplacerelations.ie) website. Built with Scrapy, MongoDB, MinIO, and Dagster.

## Tech Stack

| Tool | Purpose |
|------|---------|
| ![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python) | Core language |
| ![Scrapy](https://img.shields.io/badge/Scrapy-2.14-green?logo=scrapy) | Web scraping framework |
| ![MongoDB](https://img.shields.io/badge/MongoDB-7.0-green?logo=mongodb) | Metadata storage (NoSQL) |
| ![MinIO](https://img.shields.io/badge/MinIO-S3--compatible-red?logo=minio) | Document storage (blob/object) |
| ![Dagster](https://img.shields.io/badge/Dagster-1.6+-purple?logo=dagster) | Pipeline orchestration |
| ![Docker](https://img.shields.io/badge/Docker-compose-blue?logo=docker) | Infrastructure |
| ![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup-4.12-yellow) | HTML transformation |

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Dagster Orchestrator                     │
│                   scrape_wrc ──→ transform_wrc                  │
└──────────┬──────────────────────────────┬───────────────────────┘
           │                              │
           ▼                              ▼
┌─────────────────────┐      ┌─────────────────────────┐
│   Scrapy Spider     │      │   Transformation Script  │
│                     │      │                          │
│ • 4 bodies          │      │ • BeautifulSoup parsing  │
│ • Monthly partitions│      │ • Content extraction     │
│ • AutoThrottle      │      │ • File renaming          │
│ • Identifier skip   │      │ • Hash comparison        │
└────┬──────┬─────────┘      └────┬──────┬──────────────┘
     │      │                     │      │
     ▼      ▼                     ▼      ▼
┌─────────┐ ┌──────────┐   ┌─────────┐ ┌──────────────┐
│ MongoDB │ │  MinIO   │   │ MongoDB │ │    MinIO     │
│ landing │ │ landing- │   │ transf. │ │ transformed- │
│ metadata│ │  docs    │   │ metadata│ │    docs      │
└─────────┘ └──────────┘   └─────────┘ └──────────────┘
   Landing Zone (raw)         Transformed Zone (clean)
```

## Project Structure

```
kedra-scraper/
├── config.py                  # Central configuration (all from .env)
├── docker-compose.yaml        # MongoDB, MinIO, Dagster services
├── Dockerfile                 # Python image for Dagster containers
├── dagster.yaml               # Dagster storage config
├── scrapy.cfg                 # Scrapy project config
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables (not committed)
├── .env.example               # Template for .env
│
├── scraper/                   # Scrapy spider + pipelines
│   ├── items.py               # Item schema (metadata fields)
│   ├── settings.py            # Scrapy settings (from config.py)
│   ├── pipelines.py           # MinioPipeline + MongoPipeline
│   └── spiders/
│       └── wrc_spider.py      # Main spider (all 4 bodies)
│
├── transformation/            # Landing → Transformed zone
│   └── transform.py           # BeautifulSoup cleaning + renaming
│
├── orchestration/             # Dagster pipeline definitions
│   ├── dagster_defs.py        # Jobs, ops, config
│   └── workspace.yaml         # Dagster workspace config
│
├── utils/                     # Shared utilities
│   ├── date_utils.py          # Monthly partition generator
│   ├── hashing.py             # Content-normalized SHA-256
│   └── storage.py             # MinIO upload/download helpers
│
└── tests/                     # Test directory
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Git

### 1. Clone and Configure

```bash
git clone https://github.com/Ghantoos7/kedra-scraper.git
cd kedra-scraper
cp .env.example .env
```

### 2. Start Infrastructure

```bash
docker compose up -d mongodb minio minio-init
```

This initializes the core services required for the pipeline:

* **MongoDB** exposed on port `27018` (container runs on `27017`)
* **MinIO** object storage:

  * API → `http://localhost:9000`
  * Console → `http://localhost:9001`

---

### 3. Install Dependencies (Manual Setup)

```bash
python -m venv venv
source venv/bin/activate  # Windows: .\venv\Scripts\activate
pip install -r requirements.txt
```

This step is required only when running components locally outside Docker.

---

### 4. Run the Spider (Manual Execution)

```bash
scrapy crawl wrc_decisions -a start_date=2024-01-01 -a end_date=2024-03-31
```

Executes the scraping phase independently. Useful for testing or partial runs.

---

### 5. Run the Transformation (Manual Execution)

```bash
python -m transformation.transform --start-date 2024-01-01 --end-date 2024-03-31
```

Processes scraped data into the transformed layer. Also run manually for debugging or validation.

---

### 6. Run via Dagster (Full Pipeline - Automated)

```bash
docker compose up -d
```

This starts the full stack (including Dagster services in addition to MongoDB and MinIO).

* Open: `http://localhost:3001`
* Navigate to: **Jobs → `wrc_pipeline` → Launchpad**

This executes the full pipeline end-to-end (scraping + transformation) in an orchestrated, automated manner.


In the Dagster Launchpad, configure:

```yaml
ops:
  scrape_wrc:
    config:
      start_date: "2024-01-01"
      end_date: "2024-03-31"
      request_timeout: 60
  transform_wrc:
    config:
      start_date: "2024-01-01"
      end_date: "2024-03-31"
```

## How It Works

### Scraping (Landing Zone)

The spider scrapes all 4 bodies from the WRC website:
- Employment Appeals Tribunal
- Equality Tribunal
- Labour Court
- Workplace Relations Commission

For each body × month partition, it:
1. Fetches search result pages (with pagination)
2. Extracts metadata (identifier, description, published date, etc.)
3. Downloads the document (HTML page or PDF)
4. Computes a content-normalized SHA-256 hash
5. Stores the raw file in MinIO (`landing-docs` bucket)
6. Stores metadata in MongoDB (`landing_metadata` collection)

**Idempotency:** On startup, the spider loads all existing identifiers from MongoDB. If an identifier already exists, it skips the download entirely. The landing zone is write-once / append-only.

### Transformation (Transformed Zone)

Given a date range, the transformation script:
1. Queries `landing_metadata` by `published_date_iso`
2. For each record, checks if already transformed (hash comparison)
3. Downloads the raw file from `landing-docs`
4. If HTML: parses with BeautifulSoup, strips navbars/footers/scripts/cookies
5. If PDF/DOC: passes through unchanged
6. Renames to `identifier.ext`
7. Uploads clean file to `transformed-docs` bucket
8. Stores metadata in `transformed_metadata` collection

### Hashing Strategy

Both the spider and transformation use the same `compute_content_hash()` function from `utils/hashing.py`. For HTML files, it normalizes the content before hashing:

1. Parse with BeautifulSoup
2. Remove volatile elements (scripts, styles, nav, footer, cookie banners, hidden form fields)
3. Extract text only
4. Normalize whitespace
5. SHA-256 hash

This ensures the same logical content produces the same hash regardless of dynamic page elements (session tokens, timestamps, etc.).

### Scalability Features

| Feature | Implementation |
|---------|---------------|
| Bulk MongoDB writes | Batches of 100 operations per `bulk_write` call |
| Preloaded hash caches | O(1) identifier/hash lookups instead of per-item queries |
| Streaming cursors | `batch_size(500)` instead of loading all records into memory |
| Write-once landing | Identifier-based skip at spider level (no download for existing) |
| AutoThrottle | Dynamic rate adjustment based on server response times |
| Concurrent requests | Configurable concurrency (tested up to 64 concurrent) |

## Performance

Tested on January 2024 (278 documents):

| Concurrency | Target | Delay | Time | Retries |
|-------------|--------|-------|------|---------|
| 8 | 4 | 0.5s | 220s | 0 |
| 16 | 8 | 0.25s | 113s | 0 |
| 64 | 32 | 0.1s | 69s | 0 |
| 128 | 64 | 0s | 90s | 0 |

Sweet spot: **64 concurrent / 32 target** — fastest without server pushback.

## Configuration

All settings are in `.env` (see `.env.example`). Key parameters:

| Variable | Default | Description |
|----------|---------|-------------|
| `CONCURRENT_REQUESTS` | 64 | Max parallel requests |
| `DOWNLOAD_DELAY` | 0.1 | Base delay between requests (seconds) |
| `REQUEST_TIMEOUT` | 30 | Download timeout (seconds) |
| `AUTOTHROTTLE_TARGET_CONCURRENCY` | 32.0 | Target concurrent requests |
| `MONGO_HOST` | localhost | MongoDB host |
| `MONGO_PORT` | 27018 | MongoDB port (27018 local, 27017 in Docker) |
| `MINIO_ENDPOINT` | localhost:9000 | MinIO endpoint |

Scraping performance can also be overridden per-run in the Dagster UI config without restarting services.

## Monitoring

- **Dagster UI**: `http://localhost:3001` — Pipeline runs, logs, step status
- **MinIO Console**: `http://localhost:9001` — Browse stored documents
- **MongoDB**: `mongosh --port 27018 kedra_wrc` — Query metadata

## Error Handling

- Failed document downloads are logged with URL and error reason
- Each partition summary shows `found` vs `scraped` vs `failed` counts
- Scrapy retries failed requests 3 times (configurable)
- AutoThrottle backs off automatically when the server is slow
- Partial failures don't crash the pipeline — successful records are stored, failed ones are logged

## Design Decisions

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed rationale on:
- Monthly partition size
- Retry and rate limiting strategy
- Deduplication approach
- Scaling to 50+ sources
