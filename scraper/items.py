"""
Scrapy Item definitions for WRC decisions.

Each Item represents one decision/determination record from the
Workplace Relations website. Fields map directly to the metadata
requirements specified in the project brief 
(data model)."""


import scrapy


class WrcDecisionItem(scrapy.Item):
    """
    Metadata schema for a single WRC decision record.

    Fields:
        identifier: Decision number (e.g., ADJ-00035155, IR-SC-00001595)
        description: Party names / case description
        published_date: Date the decision was published (DD/MM/YYYY)
        ref_no: Reference number (sometimes differs from identifier)
        doc_url: Full URL to the decision document page
        body: Which body issued the decision (e.g., "Workplace Relations Commission")
        partition_date: The start date of the monthly partition this record belongs to
        file_path: Path to the downloaded file in object storage (set by pipeline)
        file_hash: SHA-256 hash of the downloaded file (set by pipeline)
        file_type: Type of downloaded file: "html" or "pdf" or "doc" (set by pipeline)
        scraped_at: ISO timestamp of when this record was scraped
    """
    # Extracted from search results page
    identifier = scrapy.Field()
    description = scrapy.Field()
    published_date = scrapy.Field()
    published_date_iso = scrapy.Field()  # YYYY-MM-DD format for sortable queries
    ref_no = scrapy.Field()
    doc_url = scrapy.Field()

    # Set by the spider based on current scrape context
    body = scrapy.Field()
    partition_date = scrapy.Field()

    # Set by the storage pipelines after downloading
    file_path = scrapy.Field()
    file_hash = scrapy.Field()
    file_type = scrapy.Field()

    # Metadata
    scraped_at = scrapy.Field()

    # Internal/temporary (used by pipelines, not stored in MongoDB)
    _file_content = scrapy.Field()
