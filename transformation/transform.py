"""
Transformation script for the Kedra WRC Scraper.

Reads raw documents from the Landing Zone (MongoDB + MinIO) and produces
cleaned documents in the Transformed Zone (new MongoDB collection + new
MinIO bucket).

Transformation steps for each document:
  1. Fetch metadata from landing_metadata (MongoDB) by date range
  2. Download the raw file from landing-docs (MinIO)
  3. If PDF/DOC → pass through unchanged
  4. If HTML → parse with BeautifulSoup, extract only relevant content
     (excludes navbars, headers, footers, cookie banners, scripts)
  5. Calculate new SHA-256 file_hash on the transformed content
  6. Rename file to identifier.ext (e.g., ADJ-00047991.html)
  7. Upload to transformed-docs (MinIO)
  8. Store updated metadata in transformed_metadata (MongoDB)

Usage:
    python -m transformation.transform --start-date 2024-01-01 --end-date 2024-01-31

The Landing Zone is NEVER modified — this is a read-only consumer of it.
"""

import argparse
import logging
import sys
from datetime import datetime

from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import MongoConfig, MinioConfig
from utils.storage import download_bytes, upload_bytes, ensure_bucket
from utils.hashing import compute_content_hash

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Transform WRC documents from Landing Zone to Transformed Zone"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD format)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD format)",
    )
    return parser.parse_args()


def connect_mongo():
    """Connect to MongoDB and return landing + transformed collections."""
    client = MongoClient(MongoConfig.connection_uri())
    db = client[MongoConfig.DB]
    landing = db[MongoConfig.LANDING_COLLECTION]
    transformed = db[MongoConfig.TRANSFORMED_COLLECTION]

    # Create indexes on transformed collection
    transformed.create_index("identifier", unique=True)
    transformed.create_index("published_date_iso")
    transformed.create_index([("body", 1), ("partition_date", 1)])

    return client, landing, transformed


def fetch_landing_cursor(collection, start_date: str, end_date: str):
    """
    Return a cursor over landing records for the given date range.

    Queries by published_date_iso (YYYY-MM-DD format) which is the actual
    publication date of the decision — not when we scraped it or which
    partition it belongs to. This matches the requirement: "Given a start
    date and end date, fetch metadata from mongo."

    Uses a cursor instead of list() to stream records without loading
    everything into memory.
    """
    query = {
        "published_date_iso": {
            "$gte": start_date,
            "$lte": end_date,
        }
    }
    count = collection.count_documents(query)
    logger.info(
        "Found %d records in landing_metadata (published between %s and %s)",
        count, start_date, end_date,
    )
    cursor = collection.find(query).batch_size(500)
    return cursor, count


def preload_transformed_hashes(collection, start_date: str, end_date: str) -> dict:
    """
    Preload existing transformed hashes into a dict for O(1) lookups.

    Queries by published_date_iso to match the same date range used
    for fetching landing records.

    Returns:
        Dict mapping identifier → original_file_hash
    """
    query = {
        "published_date_iso": {
            "$gte": start_date,
            "$lte": end_date,
        }
    }
    cache = {}
    cursor = collection.find(
        query,
        {"identifier": 1, "original_file_hash": 1, "_id": 0},
    )
    for doc in cursor:
        cache[doc["identifier"]] = doc.get("original_file_hash")

    logger.info("Preloaded %d existing transformed hashes", len(cache))
    return cache


def clean_html(raw_html: bytes) -> str:
    """
    Parse raw HTML and extract only the relevant decision content.

    Removes:
    - Navigation bars and menus
    - Cookie banners
    - Headers and footers
    - Script and style tags
    - "Return to Search" links
    - "Add To My Documents" buttons
    - Social media links

    Returns:
        Cleaned HTML string containing only the decision content.
    """
    soup = BeautifulSoup(raw_html, "lxml")

    # Remove unwanted elements
    tags_to_remove = [
        "script",
        "style",
        "nav",
        "footer",
        "header",
        "noscript",
        "iframe",
    ]
    for tag_name in tags_to_remove:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # Remove specific elements by class/id
    selectors_to_remove = [
        ".cookie-banner",
        ".navbar",
        ".footer-one",
        ".footer-two",
        ".footer-nav",
        ".social-banner",
        ".no-print",
        ".return-to-search",
        "#binderFixed",
        ".ptools-pager",
        "#advancedSearchControls",
        "#searchPnl",
        ".searchbanner",            # "This website contains decisions..." banner
        "#skippy",                  # "Skip to main content" link
        ".sr-only",                 # Screen reader only elements
    ]
    for selector in selectors_to_remove:
        for element in soup.select(selector):
            element.decompose()

    # Remove "Return to Search" links
    for a_tag in soup.find_all("a", string=lambda s: s and "Return to Search" in s):
        a_tag.decompose()

    # Try to extract the decision content area (most specific first)
    # 1. The .content div inside the page (contains the actual decision)
    # 2. The #main div (some pages use this)
    # 3. Fallback: cleaned body
    main_content = (
        soup.select_one("div.content")
        or soup.select_one("#main")
    )
    if main_content:
        cleaned_html = str(main_content)
    else:
        body = soup.find("body")
        cleaned_html = str(body) if body else str(soup)

    return cleaned_html


def transform_record(record: dict, hash_cache: dict) -> dict:
    """
    Transform a single record from the landing zone.

    Idempotency: checks the preloaded hash_cache to see if this record
    was already transformed with the same source hash. If so, skips
    entirely without downloading or parsing — O(1) lookup.

    Args:
        record: Landing zone metadata dict (from MongoDB)
        hash_cache: Preloaded dict of {identifier: original_file_hash}

    Returns:
        Transformed metadata dict, "skipped" string, or None if failed.
    """
    identifier = record.get("identifier", "unknown")
    file_path = record.get("file_path")
    file_type = record.get("file_type", "html")
    landing_hash = record.get("file_hash")

    if not file_path:
        logger.warning("No file_path for %s, skipping", identifier)
        return None

    # Idempotency check: O(1) dict lookup instead of MongoDB query
    existing_hash = hash_cache.get(identifier)
    if existing_hash is not None and existing_hash == landing_hash:
        return "skipped"

    try:
        # Download raw file from landing zone
        raw_bytes = download_bytes(MinioConfig.LANDING_BUCKET, file_path)
    except Exception as e:
        logger.error("Failed to download %s from MinIO: %s", file_path, str(e))
        return None

    # Transform based on file type
    if file_type == "html":
        # Parse and clean the HTML
        cleaned_html = clean_html(raw_bytes)
        transformed_bytes = cleaned_html.encode("utf-8")
        content_type = "text/html"
    elif file_type in ("pdf", "doc"):
        # PDF/DOC files pass through unchanged
        transformed_bytes = raw_bytes
        content_type = "application/pdf" if file_type == "pdf" else "application/msword"
    else:
        # Unknown type — pass through
        transformed_bytes = raw_bytes
        content_type = "application/octet-stream"

    # Calculate file hash on the transformed content using shared function
    transformed_hash = compute_content_hash(transformed_bytes, file_type)

    # Build new file name: identifier.ext
    # Clean identifier for filename (replace / with -, spaces with _)
    safe_identifier = identifier.replace("/", "-").replace(" ", "_")
    new_file_name = f"{safe_identifier}.{file_type}"

    # Upload to transformed zone
    try:
        new_path = upload_bytes(
            bucket=MinioConfig.TRANSFORMED_BUCKET,
            object_name=new_file_name,
            data=transformed_bytes,
            content_type=content_type,
        )
    except Exception as e:
        logger.error("Failed to upload transformed %s: %s", identifier, str(e))
        return None

    # Build transformed metadata record
    transformed_record = {
        "identifier": record.get("identifier"),
        "description": record.get("description"),
        "published_date": record.get("published_date"),
        "published_date_iso": record.get("published_date_iso"),
        "ref_no": record.get("ref_no"),
        "doc_url": record.get("doc_url"),
        "body": record.get("body"),
        "partition_date": record.get("partition_date"),
        "file_type": file_type,
        "file_hash": transformed_hash,
        "original_file_hash": landing_hash,
        "file_path": new_file_name,
        "original_file_path": record.get("file_path"),
        "transformed_at": datetime.utcnow().isoformat(),
    }

    return transformed_record


def store_transformed(collection, record: dict) -> str:
    """
    Store or update a transformed record in MongoDB.

    Uses the same idempotency logic as the landing zone:
    - New identifier → insert
    - Same identifier, different hash → update
    - Same identifier, same hash → skip

    Returns: "inserted", "updated", or "skipped"
    """
    identifier = record["identifier"]
    existing = collection.find_one({"identifier": identifier})

    if existing is None:
        collection.insert_one(record)
        return "inserted"
    elif existing.get("file_hash") != record.get("file_hash"):
        collection.update_one(
            {"identifier": identifier},
            {"$set": record},
        )
        return "updated"
    else:
        return "skipped"


def main():
    """Main transformation entry point."""
    args = parse_args()
    start_date = args.start_date
    end_date = args.end_date

    logger.info("Starting transformation: %s to %s", start_date, end_date)

    # Connect to MongoDB
    client, landing_col, transformed_col = connect_mongo()

    # Ensure transformed bucket exists
    ensure_bucket(MinioConfig.TRANSFORMED_BUCKET)

    # Preload existing transformed hashes for O(1) idempotency checks
    hash_cache = preload_transformed_hashes(transformed_col, start_date, end_date)

    # Stream records from landing zone using cursor (not list)
    cursor, total_count = fetch_landing_cursor(landing_col, start_date, end_date)

    if total_count == 0:
        logger.info("No records found in landing zone for the given date range.")
        client.close()
        return

    # Counters for summary
    stats = {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0}

    for record in cursor:
        identifier = record.get("identifier", "unknown")

        # Transform the record (idempotency check uses preloaded cache)
        transformed = transform_record(record, hash_cache)

        if transformed == "skipped":
            stats["skipped"] += 1
            continue

        if transformed is None:
            stats["failed"] += 1
            continue

        # Store in transformed collection
        try:
            result = store_transformed(transformed_col, transformed)
            stats[result] += 1

            # Update cache so subsequent runs in same session skip this record
            hash_cache[identifier] = transformed.get("original_file_hash")
        except PyMongoError as e:
            stats["failed"] += 1
            logger.error("Failed to store transformed %s: %s", identifier, str(e))

    # Log summary
    logger.info(
        "Transformation complete: total=%d, inserted=%d, updated=%d, skipped=%d, failed=%d",
        total_count, stats["inserted"], stats["updated"], stats["skipped"], stats["failed"],
    )

    client.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
