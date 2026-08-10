"""
Date utilities shared by ingestion processes.
"""

from datetime import date, timedelta

from ingestion.common.exceptions import InvalidDateRangeError


def split_date_range(
    start_date: date,
    end_date: date,
    chunk_days: int,
) -> list[tuple[date, date]]:
    """
    Split a date range into consecutive non-overlapping windows.

    Both start_date and end_date are inclusive.

    Example
    -------
    2026-01-01 -> 2026-03-31 with chunk_days=31

    produces consecutive windows containing at most 31 days.
    """

    if start_date > end_date:
        raise InvalidDateRangeError(
            f"Invalid date range: {start_date} is after {end_date}."
        )

    if chunk_days <= 0:
        raise ValueError(
            "chunk_days must be greater than zero."
        )

    chunks: list[tuple[date, date]] = []

    current_start = start_date

    while current_start <= end_date:
        current_end = min(
            current_start + timedelta(days=chunk_days - 1),
            end_date,
        )

        chunks.append(
            (current_start, current_end)
        )

        current_start = current_end + timedelta(days=1)

    return chunks