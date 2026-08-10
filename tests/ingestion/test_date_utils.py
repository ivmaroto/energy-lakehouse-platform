from datetime import date

import pytest

from ingestion.common.date_utils import split_date_range
from ingestion.common.exceptions import InvalidDateRangeError


def test_split_date_range_single_chunk():
    chunks = split_date_range(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 10),
        chunk_days=31,
    )

    assert chunks == [
        (date(2026, 1, 1), date(2026, 1, 10)),
    ]


def test_split_date_range_multiple_chunks():
    chunks = split_date_range(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 5),
        chunk_days=31,
    )

    assert chunks == [
        (date(2026, 1, 1), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 3, 3)),
        (date(2026, 3, 4), date(2026, 3, 5)),
    ]


def test_split_date_range_one_day():
    chunks = split_date_range(
        start_date=date(2026, 8, 10),
        end_date=date(2026, 8, 10),
        chunk_days=31,
    )

    assert chunks == [
        (date(2026, 8, 10), date(2026, 8, 10)),
    ]


def test_split_date_range_invalid_range():
    with pytest.raises(InvalidDateRangeError):
        split_date_range(
            start_date=date(2026, 2, 1),
            end_date=date(2026, 1, 1),
            chunk_days=31,
        )


@pytest.mark.parametrize(
    "chunk_days",
    [0, -1, -10],
)
def test_split_date_range_invalid_chunk_days(chunk_days):
    with pytest.raises(ValueError):
        split_date_range(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
            chunk_days=chunk_days,
        )