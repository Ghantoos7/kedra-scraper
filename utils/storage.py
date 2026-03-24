"""
MinIO (S3-compatible) storage utility.

Provides helper functions for uploading files to MinIO object storage.
Used by the Scrapy pipeline to store downloaded documents in the
landing-docs bucket, and by the transformation script to store
cleaned documents in the transformed-docs bucket.
"""

import io
import logging
from minio import Minio
from minio.error import S3Error

from config import MinioConfig

logger = logging.getLogger(__name__)

_client = None


def get_client() -> Minio:
    """
    Get or create a MinIO client instance.
    Reuses a single connection across the application lifetime.
    """
    global _client
    if _client is None:
        _client = Minio(
            endpoint=MinioConfig.ENDPOINT,
            access_key=MinioConfig.ACCESS_KEY,
            secret_key=MinioConfig.SECRET_KEY,
            secure=MinioConfig.SECURE,
        )
    return _client


def ensure_bucket(bucket_name: str) -> None:
    """
    Create a bucket if it doesn't already exist.
    Called at pipeline startup to ensure storage is ready.
    """
    client = get_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info("Created bucket: %s", bucket_name)


def upload_bytes(bucket: str, object_name: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    """
    Upload raw bytes to MinIO.

    Args:
        bucket: Target bucket name (e.g., "landing-docs")
        object_name: Path/name of the object in the bucket (e.g., "2024-01/ADJ-00035155.html")
        data: Raw bytes to upload
        content_type: MIME type of the content

    Returns:
        The full object path: "bucket/object_name"
    """
    client = get_client()
    data_stream = io.BytesIO(data)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=data_stream,
        length=len(data),
        content_type=content_type,
    )

    full_path = f"{bucket}/{object_name}"
    logger.debug("Uploaded %d bytes to %s", len(data), full_path)
    return full_path


def object_exists(bucket: str, object_name: str) -> bool:
    """
    Check if an object already exists in the bucket.
    Used for idempotency — skip download if file already stored.
    """
    client = get_client()
    try:
        client.stat_object(bucket_name=bucket, object_name=object_name)
        return True
    except S3Error:
        return False


def download_bytes(bucket: str, object_name: str) -> bytes:
    """
    Download an object from MinIO and return its raw bytes.
    Used by the transformation script to read from the landing zone.
    """
    client = get_client()
    response = client.get_object(bucket_name=bucket, object_name=object_name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()
