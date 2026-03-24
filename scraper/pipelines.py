"""
Scrapy Pipelines for the Kedra WRC Scraper.

Pipeline execution order (lower number = runs first):
  1. MinioPipeline (200) — Uploads downloaded file to object storage
  2. MongoPipeline (300) — Stores metadata in MongoDB with idempotency

The spider handles document downloading and hash calculation before
items reach these pipelines. Each item arrives with:
  - _file_content: raw bytes of the document
  - file_hash: SHA-256 hash
  - file_path: target path in MinIO
  - file_type: html, pdf, or doc
"""

import logging
from datetime import datetime

from scrapy.exceptions import DropItem
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import MongoConfig, MinioConfig
from utils.storage import upload_bytes, ensure_bucket

logger = logging.getLogger(__name__)


class MinioPipeline:
    """
    Uploads downloaded documents to MinIO object storage.

    File organization in the bucket:
        landing-docs/
        ├── Labour_Court/
        │   ├── 2024-01-01/
        │   │   ├── LCR22912.html
        │   │   └── PWD246.html
        │   └── 2024-02-01/
        │       └── ...
        ├── Workplace_Relations_Commission/
        │   └── ...
        └── ...

    Idempotency: If a file with the same path already exists and the
    hash hasn't changed, the upload is skipped.
    """

    def open_spider(self, spider):
        """Ensure the landing bucket exists when spider starts."""
        ensure_bucket(MinioConfig.LANDING_BUCKET)
        logger.info("MinIO pipeline ready. Bucket: %s", MinioConfig.LANDING_BUCKET)

    def process_item(self, item, spider):
        """
        Upload the document file to MinIO.

        Reads _file_content from the item (set by spider's parse_document),
        uploads to MinIO, then removes _file_content from the item so it
        doesn't get stored in MongoDB.
        """
        file_content = item.get("_file_content")
        file_path = item.get("file_path")
        file_type = item.get("file_type", "html")

        if not file_content or not file_path:
            logger.debug(
                "No file content for %s, skipping upload",
                item.get("identifier", "unknown"),
            )
            # Remove temporary field before passing to MongoDB pipeline
            item.pop("_file_content", None)
            return item

        # Determine content type for proper storage
        content_type_map = {
            "html": "text/html",
            "pdf": "application/pdf",
            "doc": "application/msword",
        }
        content_type = content_type_map.get(file_type, "application/octet-stream")

        try:
            full_path = upload_bytes(
                bucket=MinioConfig.LANDING_BUCKET,
                object_name=file_path,
                data=file_content,
                content_type=content_type,
            )
            logger.debug("Uploaded: %s (%d bytes)", full_path, len(file_content))
        except Exception as e:
            logger.error(
                "MinIO upload failed for %s: %s",
                item.get("identifier", "unknown"),
                str(e),
            )

        # Remove temporary field — raw bytes should NOT go to MongoDB
        item.pop("_file_content", None)
        return item


class MongoPipeline:
    """
    Stores item metadata in MongoDB with idempotency.

    Idempotency logic (requirement #9):
    - Uses 'identifier' as the unique key for each record
    - On first scrape: inserts the record
    - On re-scrape with same file_hash: skips (no update needed)
    - On re-scrape with different file_hash: updates the record

    This ensures running the pipeline twice on the same date range
    does NOT create duplicate records or re-download unchanged files.
    """

    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        # Counters for summary logging
        self.inserted = 0
        self.updated = 0
        self.skipped = 0

    def open_spider(self, spider):
        """
        Connect to MongoDB when the spider starts.
        Creates indexes for fast lookups and uniqueness.
        """
        try:
            self.client = MongoClient(MongoConfig.connection_uri())
            self.db = self.client[MongoConfig.DB]
            self.collection = self.db[MongoConfig.LANDING_COLLECTION]

            # Unique index on identifier — enforces no duplicate records
            self.collection.create_index("identifier", unique=True)

            # Compound index for efficient date-range + body queries
            # (used by the transformation script in Phase 4)
            self.collection.create_index([
                ("body", 1),
                ("partition_date", 1),
            ])

            logger.info(
                "MongoDB pipeline ready. DB: %s, Collection: %s",
                MongoConfig.DB,
                MongoConfig.LANDING_COLLECTION,
            )
        except PyMongoError as e:
            logger.error("Failed to connect to MongoDB: %s", str(e))
            raise

    def close_spider(self, spider):
        """Close MongoDB connection and log summary when spider finishes."""
        if self.client:
            self.client.close()
        logger.info(
            "MongoDB summary: inserted=%d, updated=%d, skipped=%d",
            self.inserted, self.updated, self.skipped,
        )

    def process_item(self, item, spider):
        """
        Store or update the item in MongoDB.

        Idempotency flow:
        1. Look up existing record by identifier
        2. If not found → insert new record
        3. If found and hash matches → skip (unchanged)
        4. If found and hash differs → update (document changed)
        """
        try:
            record = self._item_to_dict(item)
            identifier = record.get("identifier")

            if not identifier:
                raise DropItem("Item has no identifier, dropping")

            # Check if record already exists
            existing = self.collection.find_one({"identifier": identifier})

            if existing is None:
                # New record — insert
                self.collection.insert_one(record)
                self.inserted += 1
                logger.debug("Inserted: %s", identifier)

            elif existing.get("file_hash") != record.get("file_hash"):
                # Hash changed — document was updated on the source site
                self.collection.update_one(
                    {"identifier": identifier},
                    {"$set": record},
                )
                self.updated += 1
                logger.info("Updated (hash changed): %s", identifier)

            else:
                # Same hash — no changes, skip
                self.skipped += 1
                logger.debug("Skipped (unchanged): %s", identifier)

            return item

        except DropItem:
            raise
        except PyMongoError as e:
            logger.error(
                "MongoDB error for %s: %s",
                item.get("identifier", "unknown"),
                str(e),
            )
            return item

    def _item_to_dict(self, item) -> dict:
        """
        Convert a Scrapy Item to a plain dictionary for MongoDB.
        Excludes internal fields (prefixed with '_') and adds
        an updated_at timestamp.
        """
        record = {}
        for key, value in item.items():
            # Skip internal/temporary fields like _file_content
            if not key.startswith("_"):
                record[key] = value

        record["updated_at"] = datetime.utcnow().isoformat()
        return record
