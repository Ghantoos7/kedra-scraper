"""
Configuration module for the Kedra WRC Scraper.

Loads all settings from environment variables (via .env file).
This is the single source of truth — no other module should read
environment variables directly or contain hardcoded config values.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root.
# override=False ensures Docker environment variables take precedence
# over .env file values when running inside containers.
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env", override=False)


def _get_env(key: str, default: str = None, required: bool = False) -> str:
    """Retrieve an environment variable with optional default and validation."""
    value = os.getenv(key, default)
    if required and value is None:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


def _get_env_bool(key: str, default: bool = False) -> bool:
    """Retrieve a boolean environment variable."""
    return _get_env(key, str(default)).lower() in ("true", "1", "yes")


def _get_env_int(key: str, default: int = 0) -> int:
    """Retrieve an integer environment variable."""
    return int(_get_env(key, str(default)))


def _get_env_float(key: str, default: float = 0.0) -> float:
    """Retrieve a float environment variable."""
    return float(_get_env(key, str(default)))


# =============================================================================
# MongoDB Configuration
# =============================================================================
class MongoConfig:
    HOST = _get_env("MONGO_HOST", "localhost")
    PORT = _get_env_int("MONGO_PORT", 27017)
    DB = _get_env("MONGO_DB", "kedra_wrc")
    LANDING_COLLECTION = _get_env("MONGO_LANDING_COLLECTION", "landing_metadata")
    TRANSFORMED_COLLECTION = _get_env("MONGO_TRANSFORMED_COLLECTION", "transformed_metadata")

    @classmethod
    def connection_uri(cls) -> str:
        return f"mongodb://{cls.HOST}:{cls.PORT}"


# =============================================================================
# MinIO (Object Storage) Configuration
# =============================================================================
class MinioConfig:
    ENDPOINT = _get_env("MINIO_ENDPOINT", "localhost:9000")
    ACCESS_KEY = _get_env("MINIO_ACCESS_KEY", "kedra_admin")
    SECRET_KEY = _get_env("MINIO_SECRET_KEY", "kedra_secret_key_2024")
    SECURE = _get_env_bool("MINIO_SECURE", False)
    LANDING_BUCKET = _get_env("MINIO_LANDING_BUCKET", "landing-docs")
    TRANSFORMED_BUCKET = _get_env("MINIO_TRANSFORMED_BUCKET", "transformed-docs")


# =============================================================================
# Scraping Configuration
# =============================================================================
class ScrapingConfig:
    BASE_URL = _get_env("WRC_BASE_URL", "https://www.workplacerelations.ie")
    SEARCH_PATH = _get_env("WRC_SEARCH_PATH", "/en/search/")
    PARTITION_SIZE = _get_env("PARTITION_SIZE", "monthly")
    CONCURRENT_REQUESTS = _get_env_int("CONCURRENT_REQUESTS", 4)
    DOWNLOAD_DELAY = _get_env_float("DOWNLOAD_DELAY", 1.5)
    RETRY_TIMES = _get_env_int("RETRY_TIMES", 3)
    REQUEST_TIMEOUT = _get_env_int("REQUEST_TIMEOUT", 30)
    AUTOTHROTTLE_ENABLED = _get_env_bool("AUTOTHROTTLE_ENABLED", True)
    AUTOTHROTTLE_START_DELAY = _get_env_float("AUTOTHROTTLE_START_DELAY", 1)
    AUTOTHROTTLE_MAX_DELAY = _get_env_float("AUTOTHROTTLE_MAX_DELAY", 10)
    AUTOTHROTTLE_TARGET_CONCURRENCY = _get_env_float("AUTOTHROTTLE_TARGET_CONCURRENCY", 2.0)

    @classmethod
    def search_url(cls) -> str:
        return f"{cls.BASE_URL}{cls.SEARCH_PATH}"


# =============================================================================
# Body Configuration
# The 4 bodies from the WRC decisions search page.
# Each body has an ID used in the search URL query parameters.
# Adding a new body = one new entry here. Spider iterates all automatically.
# =============================================================================
class BodyConfig:
    BODIES = {
        "Employment Appeals Tribunal": _get_env("BODY_EMPLOYMENT_APPEALS_TRIBUNAL", "1"),
        "Equality Tribunal": _get_env("BODY_EQUALITY_TRIBUNAL", "2"),
        "Labour Court": _get_env("BODY_LABOUR_COURT", "3"),
        "Workplace Relations Commission": _get_env("BODY_WORKPLACE_RELATIONS_COMMISSION", "4"),
    }

    @classmethod
    def get_all(cls) -> dict:
        return cls.BODIES


# =============================================================================
# Logging Configuration
# =============================================================================
class LogConfig:
    LEVEL = _get_env("LOG_LEVEL", "INFO")
    FORMAT = _get_env("LOG_FORMAT", "json")
    FILE = _get_env("LOG_FILE", "logs/scraper.log")
