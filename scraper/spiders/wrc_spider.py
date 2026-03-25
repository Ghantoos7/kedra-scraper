"""
WRC Decisions Spider.

Scrapes decisions and determinations from the Workplace Relations
Commission website (workplacerelations.ie). Iterates over all 4 bodies
and monthly date partitions, handling pagination automatically.

URL pattern discovered from the live site:
  /en/search/?decisions=1&body={body_id}&from={DD/MM/YYYY}&to={DD/MM/YYYY}&pageNumber={n}

HTML structure (from inspecting the live site):
  Results list:    div.item-list > ul > li.each-item
  Identifier:      h2.title > a  (text and href)
  Date:            span.date
  Description:     p.description
  Ref number:      span.refNO
  View Page link:  div.link > a.btn
  Pagination:      ul.pager > li > a.next
  Total results:   div.searchhead  ("Shows 1 to 10 of 16544 results")
"""

import scrapy
import json
import logging
from datetime import date, datetime
from urllib.parse import urlencode

from pymongo import MongoClient

from scraper.items import WrcDecisionItem
from config import ScrapingConfig, BodyConfig, MongoConfig
from utils.date_utils import generate_partitions, format_date_for_wrc
from utils.hashing import compute_content_hash

logger = logging.getLogger(__name__)


class WrcDecisionsSpider(scrapy.Spider):
    """
    Spider that scrapes WRC decisions across all bodies and date partitions.

    Idempotency: Landing zone is write-once (append-only). On startup,
    the spider loads all existing identifiers from MongoDB. If an identifier
    already exists, the document is skipped entirely — no download, no storage.
    This ensures re-running on the same date range is fast and safe.

    Usage:
        scrapy crawl wrc_decisions -a start_date=2024-01-01 -a end_date=2024-12-31
    """

    name = "wrc_decisions"
    allowed_domains = ["www.workplacerelations.ie", "workplacerelations.ie"]

    def __init__(self, start_date: str = None, end_date: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not start_date or not end_date:
            raise ValueError("Both start_date and end_date are required. "
                             "Usage: scrapy crawl wrc_decisions -a start_date=2024-01-01 -a end_date=2024-12-31")

        try:
            self.start_dt = date.fromisoformat(start_date)
        except ValueError:
            raise ValueError(f"Invalid start_date: '{start_date}'. Must be YYYY-MM-DD with a valid date (e.g., 2024-02-29 not 2024-02-30).")

        try:
            self.end_dt = date.fromisoformat(end_date)
        except ValueError:
            raise ValueError(f"Invalid end_date: '{end_date}'. Must be YYYY-MM-DD with a valid date (e.g., 2024-02-29 not 2024-02-30).")

        if self.end_dt < self.start_dt:
            raise ValueError(f"end_date ({end_date}) must be after start_date ({start_date}).")
        self.base_url = ScrapingConfig.search_url()
        self.bodies = BodyConfig.get_all()

        # Preload existing identifiers for the requested date range.
        # Landing zone is append-only: never update, never delete.
        # If identifier exists → skip entirely (no download, no store).
        # We only load identifiers for the partitions we're about to process,
        # not the entire collection — scales better at 1M+ records.
        mongo_client = MongoClient(MongoConfig.connection_uri())
        collection = mongo_client[MongoConfig.DB][MongoConfig.LANDING_COLLECTION]
        cursor = collection.find(
            {
                "partition_date": {
                    "$gte": self.start_dt.isoformat(),
                    "$lte": self.end_dt.isoformat(),
                }
            },
            {"identifier": 1, "_id": 0},
        )
        self.existing_identifiers = {doc["identifier"] for doc in cursor}
        mongo_client.close()
        logger.info("Loaded %d existing identifiers for date range %s to %s",
                     len(self.existing_identifiers), start_date, end_date)

        # Statistics tracking for structured logging
        self.stats_per_partition = {}

    def start_requests(self):
        """
        Generate initial requests for every body × partition combination.

        For each body and each monthly partition, construct a search URL
        and send the first page request. This is the entry point for the
        entire crawl.
        """
        partitions = generate_partitions(self.start_dt, self.end_dt)

        logger.info(
            "Starting crawl: %d bodies × %d partitions = %d combinations",
            len(self.bodies),
            len(partitions),
            len(self.bodies) * len(partitions),
        )

        for body_name, body_id in self.bodies.items():
            for partition_start, partition_end in partitions:
                partition_key = f"{body_name}|{partition_start.isoformat()}"

                # Initialize stats tracking for this partition
                self.stats_per_partition[partition_key] = {
                    "body": body_name,
                    "partition_start": partition_start.isoformat(),
                    "partition_end": partition_end.isoformat(),
                    "total_results": 0,
                    "records_scraped": 0,
                    "records_skipped": 0,
                    "records_failed": 0,
                    "failed_urls": [],
                }

                # Build search URL for page 1
                params = {
                    "decisions": "1",
                    "body": body_id,
                    "from": format_date_for_wrc(partition_start),
                    "to": format_date_for_wrc(partition_end),
                    "pageNumber": "1",
                }
                url = f"{self.base_url}?{urlencode(params)}"

                logger.info(
                    "Queueing: body=%s, partition=%s to %s",
                    body_name,
                    partition_start.isoformat(),
                    partition_end.isoformat(),
                )

                yield scrapy.Request(
                    url=url,
                    callback=self.parse_search_results,
                    meta={
                        "body_name": body_name,
                        "body_id": body_id,
                        "partition_start": partition_start,
                        "partition_end": partition_end,
                        "partition_key": partition_key,
                        "page_number": 1,
                    },
                    errback=self.handle_error,
                )

    def parse_search_results(self, response):
        """
        Parse a search results page and extract all decision records.

        For each record on the page:
        1. Extract metadata (identifier, description, date, ref, link)
        2. Yield a WrcDecisionItem

        Then follow pagination to the next page if it exists.
        """
        meta = response.meta
        body_name = meta["body_name"]
        partition_start = meta["partition_start"]
        partition_key = meta["partition_key"]
        page_number = meta["page_number"]

        # Extract total results count from "Shows 1 to 10 of 16544 results"
        searchhead_text = response.css("div.searchhead::text").get("")
        total_results = self._extract_total_results(searchhead_text)

        if page_number == 1 and total_results is not None:
            self.stats_per_partition[partition_key]["total_results"] = total_results
            logger.info(
                "Found %d results: %s",
                total_results,
                json.dumps({"body": body_name, "partition": partition_start.isoformat(), "page": page_number, "total_results": total_results}),
            )

        # Parse each result item on the page
        items = response.css("li.each-item")

        if not items:
            logger.info(
                "No results on page: %s",
                json.dumps({"body": body_name, "partition": partition_start.isoformat(), "page": page_number}),
            )
            return

        for item_sel in items:
            try:
                decision = self._extract_item(item_sel, meta)
                if decision:
                    identifier = decision.get("identifier", "")

                    # Write-once idempotency: skip if already in landing zone
                    if identifier in self.existing_identifiers:
                        self.stats_per_partition[partition_key]["records_skipped"] += 1
                        continue

                    # New record — download the document
                    self.stats_per_partition[partition_key]["records_scraped"] += 1
                    doc_url = decision.get("doc_url", "")
                    if doc_url:
                        yield scrapy.Request(
                            url=doc_url,
                            callback=self.parse_document,
                            meta={
                                **meta,
                                "item": decision,
                            },
                            errback=self.handle_doc_error,
                        )
                    else:
                        yield decision
            except Exception as e:
                self.stats_per_partition[partition_key]["records_failed"] += 1
                failed_url = item_sel.css("h2.title a::attr(href)").get("unknown")
                self.stats_per_partition[partition_key]["failed_urls"].append({
                    "url": failed_url,
                    "error": str(e),
                })
                logger.error(
                    "Failed to extract item: body=%s, partition=%s, url=%s, error=%s",
                    body_name, partition_start.isoformat(), failed_url, str(e),
                )

        # Follow pagination — look for the "next" link
        next_page = response.css("ul.pager a.next::attr(href)").get()
        if next_page:
            next_url = response.urljoin(next_page)
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_search_results,
                meta={
                    **meta,
                    "page_number": page_number + 1,
                },
                errback=self.handle_error,
            )

    def _extract_item(self, selector, meta) -> WrcDecisionItem:
        """
        Extract a single decision record from an HTML list item.

        Args:
            selector: Scrapy Selector for one <li class="each-item">
            meta: Request meta containing body and partition info

        Returns:
            Populated WrcDecisionItem
        """
        # Identifier: from <h2 class="title"><a>ADJ-00035155</a></h2>
        identifier = selector.css("h2.title a::text").get("").strip()

        # Document URL: from the <a> tag in h2.title
        doc_path = selector.css("h2.title a::attr(href)").get("")
        doc_url = response_url = f"{ScrapingConfig.BASE_URL}{doc_path}" if doc_path else ""

        # Published date: from <span class="date">31/03/2023</span>
        published_date = selector.css("span.date::text").get("").strip()

        # Convert DD/MM/YYYY to ISO format (YYYY-MM-DD) for sortable MongoDB queries
        published_date_iso = None
        if published_date:
            try:
                day, month, year = published_date.split("/")
                published_date_iso = f"{year}-{month}-{day}"
            except (ValueError, IndexError):
                published_date_iso = None

        # Description: from <p class="description">Richard Harford V G4s...</p>
        description = selector.css("p.description::text").getall()
        description = " ".join(d.strip() for d in description if d.strip())

        # Reference number: from <span class="refNO">ADJ-00035155</span>
        ref_no = selector.css("span.refNO::text").get("").strip()

        if not identifier:
            logger.warning("Skipping item with empty identifier")
            return None

        return WrcDecisionItem(
            identifier=identifier,
            description=description,
            published_date=published_date,
            published_date_iso=published_date_iso,
            ref_no=ref_no,
            doc_url=doc_url,
            body=meta["body_name"],
            partition_date=meta["partition_start"].isoformat(),
            scraped_at=datetime.utcnow().isoformat(),
        )

    def _extract_total_results(self, text: str) -> int:
        """
        Parse total results count from searchhead text.

        Input:  "\\n            Shows 1 to\\n            10\\n\\n            of  16544  results\\n          "
        Output: 16544
        """
        try:
            if "of" in text:
                # Extract the number after "of" and before "results"
                parts = text.split("of")
                if len(parts) >= 2:
                    number_str = parts[1].strip().split()[0].strip()
                    return int(number_str)
        except (ValueError, IndexError):
            pass
        return None

    def parse_document(self, response):
        """
        Process a downloaded document page.

        Determines file type from the response headers:
        - PDF/DOC → store raw bytes as-is, hash the raw bytes
        - HTML → store the full page as .html, but hash ONLY the main
          content (excluding navbars, footers, cookies, session tokens)
          to produce a stable hash that doesn't change between requests

        Calculates SHA-256 hash and attaches everything to the item
        for the pipelines to store.
        """
        item = response.meta["item"]
        content_type = response.headers.get("Content-Type", b"").decode("utf-8", errors="ignore").lower()
        body_bytes = response.body

        # Determine file type from Content-Type header
        if "pdf" in content_type:
            file_type = "pdf"
        elif "msword" in content_type or "officedocument" in content_type:
            file_type = "doc"
        else:
            # Default to HTML (most WRC decisions are HTML pages)
            file_type = "html"

        # Calculate stable content hash using shared normalization.
        # This strips dynamic elements (cookies, session tokens, nav bars)
        # so the same logical content produces the same hash every time.
        file_hash = compute_content_hash(body_bytes, file_type)

        # Build the storage path: body/partition/identifier.ext
        body_name = item.get("body", "unknown").replace(" ", "_")
        partition = item.get("partition_date", "unknown")
        identifier = item.get("identifier", "unknown").replace("/", "-")
        object_name = f"{body_name}/{partition}/{identifier}.{file_type}"

        # Attach download info to the item
        item["file_type"] = file_type
        item["file_hash"] = file_hash
        item["file_path"] = object_name
        # Temporary field — used by MinioPipeline, not stored in MongoDB
        item["_file_content"] = body_bytes

        # Track identifier so duplicates within same run are skipped
        self.existing_identifiers.add(item.get("identifier", ""))

        yield item

    def handle_doc_error(self, failure):
        """
        Handle document download failures.
        Logs the failure as structured JSON.
        Still yields the item so metadata is saved, but without file info.
        """

        request = failure.request
        meta = request.meta
        item = meta.get("item")
        partition_key = meta.get("partition_key", "unknown")

        error_record = {
            "event": "document_download_failed",
            "identifier": item.get("identifier", "unknown") if item else "unknown",
            "url": request.url,
            "error": str(failure.value),
            "body": meta.get("body_name", "unknown"),
            "partition": meta.get("partition_start", "unknown").isoformat() if hasattr(meta.get("partition_start", ""), "isoformat") else str(meta.get("partition_start", "unknown")),
        }
        logger.error("Document download failed: %s", json.dumps(error_record))

        if partition_key in self.stats_per_partition:
            self.stats_per_partition[partition_key]["records_failed"] += 1
            self.stats_per_partition[partition_key]["failed_urls"].append({
                "url": request.url,
                "error": str(failure.value),
            })

        # Still yield the item with metadata, just without file info
        if item:
            item["file_type"] = "unknown"
            item["file_hash"] = None
            item["file_path"] = None
            yield item

    def handle_error(self, failure):
        """
        Handle search page request failures (timeouts, connection errors).
        Logs the failure as structured JSON.
        """

        request = failure.request
        meta = request.meta
        partition_key = meta.get("partition_key", "unknown")

        error_record = {
            "event": "request_failed",
            "url": request.url,
            "error": str(failure.value),
            "body": meta.get("body_name", "unknown"),
            "partition": meta.get("partition_start", "unknown").isoformat() if hasattr(meta.get("partition_start", ""), "isoformat") else str(meta.get("partition_start", "unknown")),
        }
        logger.error("Request failed: %s", json.dumps(error_record))

        if partition_key in self.stats_per_partition:
            self.stats_per_partition[partition_key]["records_failed"] += 1
            self.stats_per_partition[partition_key]["failed_urls"].append({
                "url": request.url,
                "error": str(failure.value),
            })

    def closed(self, reason):
        """
        Called when the spider finishes. Produces the structured log summary
        required by the project brief.
        """


        total_scraped = 0
        total_skipped = 0
        total_failed = 0
        total_expected = 0

        for key, stats in self.stats_per_partition.items():
            total_scraped += stats["records_scraped"]
            total_skipped += stats["records_skipped"]
            total_failed += stats["records_failed"]
            total_expected += stats["total_results"]

            # Log each partition's stats
            logger.info("Partition summary: %s", json.dumps(stats))

        summary = {
            "event": "crawl_complete",
            "reason": reason,
            "total_expected": total_expected,
            "total_scraped": total_scraped,
            "total_skipped": total_skipped,
            "total_failed": total_failed,
            "partitions_processed": len(self.stats_per_partition),
        }
        logger.info("Crawl summary: %s", json.dumps(summary))
