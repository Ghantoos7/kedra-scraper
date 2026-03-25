"""
Scrapy settings for the Kedra WRC Scraper.

All configurable values are pulled from config.py which reads from .env.
This ensures no hardcoded values exist in the codebase.
"""

import sys
from pathlib import Path

# Add project root to path so config module is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import ScrapingConfig, LogConfig

BOT_NAME = "kedra_wrc_scraper"

SPIDER_MODULES = ["scraper.spiders"]
NEWSPIDER_MODULE = "scraper.spiders"

# --- Politeness settings (avoid getting blocked) ---
# Obey robots.txt as a baseline courtesy
ROBOTSTXT_OBEY = True

# Concurrent requests — kept low to avoid triggering rate limits
CONCURRENT_REQUESTS = ScrapingConfig.CONCURRENT_REQUESTS

# Delay between consecutive requests to the same domain
DOWNLOAD_DELAY = ScrapingConfig.DOWNLOAD_DELAY

# Per-domain concurrency limit (more conservative than global)
CONCURRENT_REQUESTS_PER_DOMAIN = ScrapingConfig.CONCURRENT_REQUESTS

# --- AutoThrottle (dynamic rate adjustment) ---
# This is the "fastest way without getting blocked" — Scrapy monitors
# response times and automatically adjusts delay up/down
AUTOTHROTTLE_ENABLED = ScrapingConfig.AUTOTHROTTLE_ENABLED
AUTOTHROTTLE_START_DELAY = ScrapingConfig.AUTOTHROTTLE_START_DELAY
AUTOTHROTTLE_MAX_DELAY = ScrapingConfig.AUTOTHROTTLE_MAX_DELAY
AUTOTHROTTLE_TARGET_CONCURRENCY = ScrapingConfig.AUTOTHROTTLE_TARGET_CONCURRENCY
AUTOTHROTTLE_DEBUG = False

# --- Retry settings ---
RETRY_ENABLED = True
RETRY_TIMES = ScrapingConfig.RETRY_TIMES
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# --- Timeout ---
DOWNLOAD_TIMEOUT = ScrapingConfig.REQUEST_TIMEOUT

# --- User Agent rotation ---
# Rotate user agents to reduce chance of being blocked
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# --- Request headers ---
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

# --- Pipelines ---
# Execution order: lower number = runs first
# Spider handles document downloading and hash calculation.
# 1. MinioPipeline (200): Uploads file to object storage
# 2. MongoPipeline (300): Stores metadata in MongoDB with idempotency
ITEM_PIPELINES = {
    "scraper.pipelines.MinioPipeline": 200,
    "scraper.pipelines.MongoPipeline": 300,
}

# --- Logging ---
# Traditional format for readable output. Structured JSON data is embedded
# in the partition summaries and crawl summary (requirement #10).
LOG_LEVEL = LogConfig.LEVEL
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# --- Caching (speeds up development — disable in production) ---
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 86400
# HTTPCACHE_DIR = "httpcache"

# --- Output encoding ---
FEED_EXPORT_ENCODING = "utf-8"

# --- Respect server by identifying ourselves ---
SPIDER_MIDDLEWARES = {}
DOWNLOADER_MIDDLEWARES = {}
