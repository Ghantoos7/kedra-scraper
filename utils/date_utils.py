"""
Date partitioning utility.

Generates monthly (or other sized) date partitions between a start
and end date. Used by the spider to break large date ranges into
smaller chunks for the WRC search filters.

Why monthly? The WRC site returns max 10 results per page. For bodies
with thousands of records, scraping all pages for the entire date range
would be slow and risky (more chance of failures). Monthly partitions:
- Keep each partition's result count manageable
- Allow parallel scraping of different partitions
- Make the pipeline resumable (re-run only failed months)
- Add a natural partition_date field for data organization
"""

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Tuple

from config import ScrapingConfig


def generate_partitions(
    start_date: date,
    end_date: date,
    partition_size: str = None,
) -> List[Tuple[date, date]]:
    """
    Generate date range partitions between start_date and end_date.

    Args:
        start_date: Beginning of the overall date range.
        end_date: End of the overall date range.
        partition_size: "monthly", "weekly", or "quarterly".
                        Defaults to config value.

    Returns:
        List of (partition_start, partition_end) tuples.
        Each tuple represents one partition's date range.

    Example:
        >>> generate_partitions(date(2024, 1, 1), date(2024, 3, 1), "monthly")
        [(date(2024, 1, 1), date(2024, 1, 31)),
         (date(2024, 2, 1), date(2024, 2, 29)),
         (date(2024, 3, 1), date(2024, 3, 31))]
    """
    if partition_size is None:
        partition_size = ScrapingConfig.PARTITION_SIZE

    partitions = []
    current = start_date

    while current <= end_date:
        if partition_size == "monthly":
            # Partition end = last day of the current month
            next_start = current + relativedelta(months=1, day=1)
            partition_end = next_start - timedelta(days=1)
        elif partition_size == "weekly":
            partition_end = current + timedelta(days=6)
        elif partition_size == "quarterly":
            next_start = current + relativedelta(months=3, day=1)
            partition_end = next_start - timedelta(days=1)
        else:
            raise ValueError(f"Unknown partition size: {partition_size}")

        # Don't exceed the overall end date
        partition_end = min(partition_end, end_date)
        partitions.append((current, partition_end))

        # Move to the start of the next partition
        current = partition_end + timedelta(days=1)

    return partitions


def format_date_for_wrc(d: date) -> str:
    """
    Format a date for the WRC search URL query parameters.
    WRC expects DD/MM/YYYY format.

    Args:
        d: Python date object.

    Returns:
        Date string in DD/MM/YYYY format.
    """
    return d.strftime("%d/%m/%Y")
