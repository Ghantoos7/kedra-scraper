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
import logging
from datetime import date, datetime
from urllib.parse import urlencode

from scraper.items import WrcDecisionItem
from config import ScrapingConfig, BodyConfig
from utils.date_utils import generate_partitions, format_date_for_wrc

logger = logging.getLogger(__name__)


class WrcDecisionsSpider(scrapy.Spider):
    """
    Spider that scrapes WRC decisions across all bodies and date partitions.

    Usage:
        scrapy crawl wrc_decisions -a start_date=2024-01-01 -a end_date=2024-12-31

    Arguments:
        start_date: Start of date range (YYYY-MM-DD format)
        end_date: End of date range (YYYY-MM-DD format)
    """

    name = "wrc_decisions"
    allowed_domains = ["www.workplacerelations.ie", "workplacerelations.ie"]

    def __init__(self, start_date: str = None, end_date: str = None, *args, **kwargs):
        """
        Initialize spider with date range arguments.

        Args:
            start_date: Required. Format: YYYY-MM-DD
            end_date: Required. Format: YYYY-MM-DD
        """
        super().__init__(*args, **kwargs)

        if not start_date or not end_date:
            raise ValueError("Both start_date and end_date are required. "
                             "Usage: scrapy crawl wrc_decisions -a start_date=2024-01-01 -a end_date=2024-12-31")

        self.start_dt = date.fromisoformat(start_date)
        self.end_dt = date.fromisoformat(end_date)
        self.base_url = ScrapingConfig.search_url()
        self.bodies = BodyConfig.get_all()

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
                "Found %d results: body=%s, partition=%s, page=%d",
                total_results, body_name, partition_start.isoformat(), page_number,
            )

        # Parse each result item on the page
        items = response.css("li.each-item")

        if not items:
            logger.info(
                "No results on page: body=%s, partition=%s, page=%d",
                body_name, partition_start.isoformat(), page_number,
            )
            return

        for item_sel in items:
            try:
                decision = self._extract_item(item_sel, meta)
                if decision:
                    self.stats_per_partition[partition_key]["records_scraped"] += 1
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

    def handle_error(self, failure):
        """
        Handle request failures (timeouts, connection errors, etc.).
        Logs the failure with the URL and error for the structured log summary.
        """
        request = failure.request
        meta = request.meta
        partition_key = meta.get("partition_key", "unknown")

        if partition_key in self.stats_per_partition:
            self.stats_per_partition[partition_key]["records_failed"] += 1
            self.stats_per_partition[partition_key]["failed_urls"].append({
                "url": request.url,
                "error": str(failure.value),
            })

        logger.error(
            "Request failed: url=%s, error=%s",
            request.url, str(failure.value),
        )

    def closed(self, reason):
        """
        Called when the spider finishes. Produces the structured log summary
        required by the project brief.
        """
        import json

        total_scraped = 0
        total_failed = 0
        total_expected = 0

        for key, stats in self.stats_per_partition.items():
            total_scraped += stats["records_scraped"]
            total_failed += stats["records_failed"]
            total_expected += stats["total_results"]

            # Log each partition's stats
            logger.info("Partition summary: %s", json.dumps(stats))

        summary = {
            "event": "crawl_complete",
            "reason": reason,
            "total_expected": total_expected,
            "total_scraped": total_scraped,
            "total_failed": total_failed,
            "partitions_processed": len(self.stats_per_partition),
        }
        logger.info("Crawl summary: %s", json.dumps(summary))
