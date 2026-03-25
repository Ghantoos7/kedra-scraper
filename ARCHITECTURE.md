# Architecture

## Partition Strategy: Monthly

Monthly partitions balance two competing concerns:

**Too granular (daily):** 365 partitions/year × 4 bodies = 1,460 search page requests just for one year. Most days have 0 results so its a lot of wasted HTTP requests.

**Too coarse (yearly):** The WRC search page paginates at 10 results per page. This means that for a single year we could have thousands of records for example 10,000 a year so 1000 pages. If page 6000 fails, you lose half the partition with no easy way to resume, we would have to restart the process on that partition.

**Monthly is the sweet spot:** Each partition has lower amount of records then the yearly (divided by 12). A failed partition is cheap to retry. The spider generates `4 bodies × 12 months = 48` initial requests per year — manageable concurrency.

## Retries and Rate Limiting

The pipeline uses three layers of protection against being blocked:

1. **AutoThrottle (dynamic):** Scrapy monitors server response times and adjusts request delay automatically. If the server slows down, Scrapy backs off. If it speeds up, Scrapy sends faster. This is the "fastest without getting blocked" approach.
```
AUTOTHROTTLE_START_DELAY=1 
AUTOTHROTTLE_MAX_DELAY=10
``` 

2. **Retry with backoff:** Failed requests (timeout, 5xx, 429) are retried 3 times. Scrapy's built-in retry middleware handles this with exponential backoff.

3. **Configurable concurrency:** Tested from 8 to 128 concurrent requests. The WRC server handles 64 concurrent well but pushes back at 128. Settings are configurable via `.env` or Dagster UI , no code changes needed to adjust for different servers.

## Deduplication Strategy

The landing zone uses **write-once, identifier-based deduplication:**

1. On spider startup, all existing identifiers are loaded into an in-memory `set` (O(1) lookups)
2. For each record on the search page, the spider checks: `identifier in existing_set?`
3. If yes → skip entirely (no HTTP request, no download, no storage)
4. If no → download, hash, store, add to set

This means re-running the spider on the same date range triggers zero downloads, only the lightweight search page requests to check for new records.

The transformation layer uses **hash-based deduplication:**

1. Preloads `{identifier: original_file_hash}` from the transformed collection
2. For each landing record: if the landing hash matches the stored original hash → skip
3. Only re-transforms when the source has changed

Both layers use preloaded in-memory caches for O(1) lookups. No per-item database queries.

I also considered tracking document changes via hash comparison on re-scrape (detecting when the WRC updates a decision). After discussing with Daniel, we decided this edge case is rare enough that the write-once model is sufficient. If needed, 

## Content Hashing

HTML pages contain dynamic elements (session tokens, cookie banners, timestamps) that change every request. Hashing raw bytes would produce different hashes for identical content.

The solution: a shared `compute_content_hash()` function that normalizes HTML before hashing, strips scripts, styles, nav, footer, cookie banners, hidden form fields, then extracts text content and normalizes whitespace. Both the spider and transformation use this same function, ensuring consistent hashes across the pipeline.

## Scaling to 50+ Sources

The current architecture is designed for one source (WRC) but the patterns generalize:

**What would change:**

- **Spider registry:** Instead of one `wrc_spider.py`, a base spider class with source-specific subclasses. Each source defines its own CSS selectors, URL patterns, and pagination logic (basically standardize them). New sources are added by creating a new spider file, the cool part is no changes needed to the pipeline or infrastructure.

- **Source-specific configuration:** Each source gets its own `BodyConfig` with rate limits, concurrency, and timeout tuned to that server's capacity. Stored in a config file or database.

- **Parallel execution:** Dagster would run each source as a separate op or asset, enabling parallel scraping across sources while respecting per-source rate limits. Failed sources don't block others.

- **Shared storage with namespacing:** MinIO paths already include body/partition. Adding a source prefix (`wrc/`, `source2/`, etc.) separates documents naturally. MongoDB collections could be per-source or use a `source` field with compound indexes.

- **Proxy rotation:** For 50+ sources, some servers will block repeated requests from the same IP. A proxy rotation service (Scrapy middlewares like `scrapy-rotating-proxies`) would distribute requests across IP addresses.

**What stays the same:**

- The landing/transformed two-zone architecture
- Dagster orchestration with dependency handling
- Content-normalized hashing for idempotency
- MongoDB + MinIO storage pattern
- BeautifulSoup transformation with source-specific selectors
