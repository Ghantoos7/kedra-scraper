"""
Scrapy Pipelines for the Kedra WRC Scraper.

Pipeline execution order (lower number = runs first):
  1. MinioPipeline (200) — Uploads downloaded file to object storage
  2. MongoPipeline (300) — Stores metadata in MongoDB with idempotency

Optimizations for scale (designed for 1M+ records):
  - MongoPipeline preloads existing hashes into memory at startup
    for O(1) idempotency lookups (no per-item find_one queries)
  - Bulk write operations flush every BATCH_SIZE items instead of
    individual insert/update per item
  - MinIO uploads are per-item (can't be batched due to binary data)
"""

import logging
from datetime import datetime

from scrapy.exceptions import DropItem
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import PyMongoError, BulkWriteError

from config import MongoConfig, MinioConfig
from utils.storage import upload_bytes, ensure_bucket

logger = logging.getLogger(__name__)

# Batch size for bulk MongoDB writes
MONGO_BATCH_SIZE = 100


class MinioPipeline:
    """
    Uploads downloaded documents to MinIO object storage.

    File organization in the bucket:
        landing-docs/
        ├── Labour_Court/
        │   └── 2024-01-01/
        │       ├── LCR22912.html
        │       └── PWD246.html
        └── Workplace_Relations_Commission/
            └── ...
    """

    def open_spider(self, spider):
        """Ensure the landing bucket exists when spider starts."""
        ensure_bucket(MinioConfig.LANDING_BUCKET)
        logger.info("MinIO pipeline ready. Bucket: %s", MinioConfig.LANDING_BUCKET)

    def process_item(self, item, spider):
        """
        Upload the document file to MinIO.
        Skips upload if no file content is attached.
        """
        file_content = item.get("_file_content")
        file_path = item.get("file_path")
        file_type = item.get("file_type", "html")

        if not file_content or not file_path:
            item.pop("_file_content", None)
            return item

        content_type_map = {
            "html": "text/html",
            "pdf": "application/pdf",
            "doc": "application/msword",
        }
        content_type = content_type_map.get(file_type, "application/octet-stream")

        try:
            upload_bytes(
                bucket=MinioConfig.LANDING_BUCKET,
                object_name=file_path,
                data=file_content,
                content_type=content_type,
            )
        except Exception as e:
            logger.error(
                "MinIO upload failed for %s: %s",
                item.get("identifier", "unknown"),
                str(e),
            )

        # Remove raw bytes before passing to MongoDB pipeline
        item.pop("_file_content", None)
        return item


class MongoPipeline:
    """
    Stores item metadata in MongoDB with idempotency.

    Scalability optimizations:
    1. On spider open: preloads ALL existing {identifier: file_hash} pairs
       into an in-memory dict. This avoids per-item find_one queries.
       For 1M records, this dict uses ~100MB of RAM (acceptable).

    2. Items are collected into a batch buffer. Every BATCH_SIZE items,
       the buffer is flushed to MongoDB using bulk_write (single round-trip
       for N operations instead of N round-trips).

    3. Remaining items are flushed on spider close.

    Idempotency flow (per item, in memory):
      - identifier not in cache → queue InsertOne
      - identifier in cache, hash differs → queue UpdateOne
      - identifier in cache, hash matches → skip (no DB call)
    """

    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        # In-memory cache: {identifier: file_hash}
        self.hash_cache = {}
        # Batch buffer for bulk writes
        self.batch_buffer = []
        # Counters
        self.inserted = 0
        self.updated = 0
        self.skipped = 0

    def open_spider(self, spider):
        """
        Connect to MongoDB, create indexes, and preload existing hashes.
        """
        try:
            self.client = MongoClient(MongoConfig.connection_uri())
            self.db = self.client[MongoConfig.DB]
            self.collection = self.db[MongoConfig.LANDING_COLLECTION]

            # Create indexes
            self.collection.create_index("identifier", unique=True)
            self.collection.create_index([("body", 1), ("partition_date", 1)])

            # Index for transformation queries by published date
            self.collection.create_index("published_date_iso")

            # Preload existing hashes for O(1) idempotency checks.
            # Only loads identifier and file_hash — not full documents.
            cursor = self.collection.find(
                {},
                {"identifier": 1, "file_hash": 1, "_id": 0},
            )
            for doc in cursor:
                self.hash_cache[doc["identifier"]] = doc.get("file_hash")

            logger.info(
                "MongoDB pipeline ready. Preloaded %d existing hashes. DB: %s, Collection: %s",
                len(self.hash_cache),
                MongoConfig.DB,
                MongoConfig.LANDING_COLLECTION,
            )
        except PyMongoError as e:
            logger.error("Failed to connect to MongoDB: %s", str(e))
            raise

    def close_spider(self, spider):
        """Flush remaining batch and close connection."""
        # Flush any remaining items in the buffer
        if self.batch_buffer:
            self._flush_batch()

        if self.client:
            self.client.close()

        logger.info(
            "MongoDB summary: inserted=%d, updated=%d, skipped=%d",
            self.inserted, self.updated, self.skipped,
        )

    def process_item(self, item, spider):
        """
        Check idempotency in memory, queue write operation, flush when batch is full.
        """
        try:
            record = self._item_to_dict(item)
            identifier = record.get("identifier")

            if not identifier:
                raise DropItem("Item has no identifier, dropping")

            existing_hash = self.hash_cache.get(identifier)

            if existing_hash is None:
                # New record → queue insert
                self.batch_buffer.append(("insert", record))
                self.hash_cache[identifier] = record.get("file_hash")
                self.inserted += 1

            elif existing_hash != record.get("file_hash"):
                # Hash changed → queue update
                self.batch_buffer.append(("update", record))
                self.hash_cache[identifier] = record.get("file_hash")
                self.updated += 1

            else:
                # Same hash → skip entirely (no DB call)
                self.skipped += 1

            # Flush batch when it reaches the threshold
            if len(self.batch_buffer) >= MONGO_BATCH_SIZE:
                self._flush_batch()

            return item

        except DropItem:
            raise
        except Exception as e:
            logger.error(
                "MongoDB error for %s: %s",
                item.get("identifier", "unknown"),
                str(e),
            )
            return item

    def _flush_batch(self):
        """
        Execute all queued operations in a single bulk_write call.
        One network round-trip for up to BATCH_SIZE operations.
        """
        if not self.batch_buffer:
            return

        operations = []
        for op_type, record in self.batch_buffer:
            identifier = record["identifier"]
            if op_type == "insert":
                operations.append(
                    UpdateOne(
                        {"identifier": identifier},
                        {"$setOnInsert": record},
                        upsert=True,
                    )
                )
            elif op_type == "update":
                operations.append(
                    UpdateOne(
                        {"identifier": identifier},
                        {"$set": record},
                    )
                )

        try:
            if operations:
                result = self.collection.bulk_write(operations, ordered=False)
                logger.debug(
                    "Bulk write: %d ops (upserted=%d, modified=%d)",
                    len(operations),
                    result.upserted_count,
                    result.modified_count,
                )
        except BulkWriteError as e:
            logger.error("Bulk write error: %s", str(e.details))
        except PyMongoError as e:
            logger.error("MongoDB bulk write failed: %s", str(e))

        self.batch_buffer.clear()

    def _item_to_dict(self, item) -> dict:
        """
        Convert a Scrapy Item to a plain dictionary for MongoDB.
        Excludes internal fields (prefixed with '_').
        """
        record = {}
        for key, value in item.items():
            if not key.startswith("_"):
                record[key] = value
        record["updated_at"] = datetime.utcnow().isoformat()
        return record
