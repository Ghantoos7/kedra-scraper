"""
Dagster pipeline definitions for the Kedra WRC Scraper.

Orchestrates two separate tasks with dependency handling:
  1. scrape_wrc — Runs the Scrapy spider into the Landing Zone
  2. transform_wrc — Cleans documents into the Transformed Zone

Logs are streamed in real-time (not buffered until completion) so
operators can monitor progress during long-running scrapes.
"""

import subprocess
import sys
import os
import re
import json

from dagster import (
    Definitions,
    OpExecutionContext,
    job,
    op,
    Config,
    In,
    Nothing,
)


class ScrapeConfig(Config):
    """Configuration for the scraping task."""
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"
    # Scraping performance — override from Dagster UI without restarting.
    # 0 = use .env default value.
    concurrent_requests: int = 0
    download_delay: float = 0
    request_timeout: int = 0
    autothrottle_target: float = 0


class TransformConfig(Config):
    """Configuration for the transformation task."""
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"


def _get_project_root():
    """Get the project root directory (one level up from orchestration/)."""
    from pathlib import Path
    return Path(__file__).parent.parent


def _get_docker_env(config: ScrapeConfig = None) -> dict:
    """
    Build environment dict for subprocess calls.

    1. Starts with current process environment
    2. Fixes Docker networking (mongodb/minio hostnames)
    3. Applies any Dagster UI config overrides for scraping performance
    """
    env = os.environ.copy()
    # Force unbuffered output so logs stream in real-time
    env["PYTHONUNBUFFERED"] = "1"
    if env.get("DAGSTER_HOME", "").startswith("/opt/dagster"):
        env["MONGO_HOST"] = "mongodb"
        env["MONGO_PORT"] = "27017"
        env["MINIO_ENDPOINT"] = "minio:9000"

    # Apply Dagster config overrides (0 = use .env default)
    if config:
        if config.concurrent_requests > 0:
            env["CONCURRENT_REQUESTS"] = str(config.concurrent_requests)
        if config.download_delay > 0:
            env["DOWNLOAD_DELAY"] = str(config.download_delay)
        if config.request_timeout > 0:
            env["REQUEST_TIMEOUT"] = str(config.request_timeout)
        if config.autothrottle_target > 0:
            env["AUTOTHROTTLE_TARGET_CONCURRENCY"] = str(config.autothrottle_target)

    return env


# ─── Log line keywords ──────────────────────────────────────────────────────
# Only lines matching these keywords are shown in the Dagster UI.
# Everything else (middleware lists, settings dumps, deprecation warnings) is filtered.

SCRAPE_SHOW_KEYWORDS = [
    "Spider opened",
    "Spider closed",
    "Starting crawl",
    "Loaded",
    "Queueing:",
    "Found",
    "No results",
    "MongoDB pipeline ready",
    "MongoDB summary",
    "MinIO pipeline ready",
    "Partition summary",
    "Crawl summary",
    "Gave up retrying",
    "Request failed",
    "Document download failed",
    "Crawled",
    "items/min",
]

TRANSFORM_SHOW_KEYWORDS = [
    "Starting transformation",
    "Found",
    "Preloaded",
    "Transformation complete",
    "Done",
    "ERROR",
    "Failed",
]


def _should_show_line(msg: str, keywords: list) -> bool:
    """Check if a log message matches any of the show keywords."""
    return any(kw in msg for kw in keywords)


def _parse_log_line(line: str) -> tuple:
    """
    Extract level and message from a log line.

    Handles two formats:
    - JSON: {"timestamp": "...", "level": "INFO", "message": "..."}
    - Traditional: "2026-03-25 11:21:42 [logger] INFO: message"

    Returns (level, message) or (None, None) if unparseable.
    """
    line = line.strip()
    if not line:
        return None, None

    # Try JSON format first
    try:
        log_obj = json.loads(line)
        return log_obj.get("level", "INFO"), log_obj.get("message", "")
    except (json.JSONDecodeError, ValueError):
        pass

    # Fallback: traditional Scrapy format
    msg_match = re.search(r"\]\s+(INFO|ERROR|WARNING):\s+(.+)$", line)
    if msg_match:
        return msg_match.group(1), msg_match.group(2)

    return None, None


def _format_json_summary(msg: str) -> str:
    """
    Convert raw JSON partition/crawl summaries into readable format.

    Input:  'Partition summary: {"body": "Labour Court", ...}'
    Output: 'Labour Court | 2024-01-01 to 2024-01-31 | found: 45, new: 10, skipped: 35, failed: 0'
    """
    try:
        json_start = msg.index("{")
        data = json.loads(msg[json_start:])

        if "body" in data:
            body = data.get("body", "?")
            start = data.get("partition_start", "?")
            end = data.get("partition_end", "?")
            total = data.get("total_results", 0)
            scraped = data.get("records_scraped", 0)
            skipped = data.get("records_skipped", 0)
            failed = data.get("records_failed", 0)
            failed_urls = data.get("failed_urls", [])

            result = f"{body} | {start} to {end} | found: {total}, new: {scraped}, skipped: {skipped}, failed: {failed}"
            if failed_urls:
                for f in failed_urls[:3]:
                    result += f"\n    FAILED: {f.get('url', '?')} — {f.get('error', '?')}"
            return result

        elif "event" in data:
            expected = data.get("total_expected", 0)
            scraped = data.get("total_scraped", 0)
            skipped = data.get("total_skipped", 0)
            failed = data.get("total_failed", 0)
            partitions = data.get("partitions_processed", 0)
            return (
                f"Crawl complete | partitions: {partitions}, "
                f"found: {expected}, new: {scraped}, skipped: {skipped}, failed: {failed}"
            )
    except (ValueError, json.JSONDecodeError):
        pass
    return msg


def _log_line(context: OpExecutionContext, level: str, msg: str):
    """Log a message at the appropriate level."""
    if level == "ERROR":
        context.log.error(msg)
    elif level == "WARNING":
        context.log.warning(msg)
    else:
        context.log.info(msg)


def _stream_subprocess(
    context: OpExecutionContext, cmd: list, label: str,
    keywords: list, config: ScrapeConfig = None,
) -> tuple:
    """
    Run a subprocess and stream its output line-by-line to Dagster logs.

    Uses Popen instead of subprocess.run so logs appear in real-time
    in the Dagster UI, not buffered until the process finishes.

    Returns:
        Tuple of (return_code, stderr_text)
    """
    env = _get_docker_env(config)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(_get_project_root()),
        env=env,
        bufsize=1,  # Line-buffered
    )

    # Collect all stderr for post-processing (stats extraction)
    all_stderr = []

    # Stream stderr line by line (Scrapy writes to stderr)
    for line in process.stderr:
        all_stderr.append(line)
        level, msg = _parse_log_line(line)
        if not msg:
            continue

        if _should_show_line(msg, keywords):
            if "Partition summary:" in msg or "Crawl summary:" in msg:
                msg = _format_json_summary(msg)
            _log_line(context, level, msg)

    # Also stream stdout (transformation script writes here)
    for line in process.stdout:
        level, msg = _parse_log_line(line)
        if msg and _should_show_line(msg, keywords):
            _log_line(context, level or "INFO", msg)

    process.wait()
    return process.returncode, "".join(all_stderr)


def _extract_scrapy_stats(stderr: str) -> dict:
    """Extract key stats from Scrapy's stats dump."""
    stats = {}
    patterns = {
        "items": r"'item_scraped_count':\s*(\d+)",
        "elapsed": r"'elapsed_time_seconds':\s*([\d.]+)",
        "requests": r"'downloader/request_count':\s*(\d+)",
        "retries": r"'retry/count':\s*(\d+)",
        "errors": r"'log_count/ERROR':\s*(\d+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, stderr)
        if match:
            stats[key] = match.group(1)
    return stats


@op
def scrape_wrc(context: OpExecutionContext, config: ScrapeConfig):
    """
    Scraping task — runs the Scrapy spider as a subprocess.

    Logs are streamed in real-time so progress is visible in the
    Dagster UI during long-running scrapes. Subprocess isolation
    prevents Scrapy's Twisted reactor from conflicting with Dagster.
    """
    start_date = config.start_date
    end_date = config.end_date
    context.log.info(f"Scraping WRC decisions: {start_date} to {end_date}")

    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "wrc_decisions",
        "-a", f"start_date={start_date}",
        "-a", f"end_date={end_date}",
    ]

    returncode, stderr = _stream_subprocess(
        context, cmd, "scrape", SCRAPE_SHOW_KEYWORDS, config=config
    )

    # Log summary stats
    stats = _extract_scrapy_stats(stderr)
    if stats:
        context.log.info(
            f"Scrape stats: items={stats.get('items', '0')}, "
            f"time={stats.get('elapsed', '?')}s, "
            f"requests={stats.get('requests', '?')}, "
            f"retries={stats.get('retries', '0')}, "
            f"errors={stats.get('errors', '0')}"
        )

    if returncode != 0:
        raise Exception(f"Scraping failed (exit code {returncode})")

    context.log.info("Scraping step completed successfully")


@op(ins={"scrape_result": In(Nothing)})
def transform_wrc(context: OpExecutionContext, config: TransformConfig):
    """
    Transformation task — cleans documents from Landing to Transformed Zone.

    Depends on scrape_wrc — Dagster runs this only after scraping succeeds.
    """
    start_date = config.start_date
    end_date = config.end_date
    context.log.info(f"Transforming WRC decisions: {start_date} to {end_date}")

    cmd = [
        sys.executable, "-m", "transformation.transform",
        "--start-date", start_date,
        "--end-date", end_date,
    ]

    returncode, _ = _stream_subprocess(
        context, cmd, "transform", TRANSFORM_SHOW_KEYWORDS
    )

    if returncode != 0:
        raise Exception(f"Transformation failed (exit code {returncode})")

    context.log.info("Transformation step completed successfully")


@job
def wrc_pipeline():
    """
    Full WRC pipeline: scrape → transform.

    Dependency: transform_wrc runs only after scrape_wrc succeeds.
    Both ops receive the same start_date/end_date configuration.
    """
    transform_wrc(scrape_result=scrape_wrc())


# ─── Dagster Definitions ────────────────────────────────────────────────────
defs = Definitions(
    jobs=[wrc_pipeline],
)
