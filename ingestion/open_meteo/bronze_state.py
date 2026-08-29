"""
Bronze state inspection for resumable Open-Meteo ingestion.
"""

from __future__ import annotations

import json

from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)
from typing import Iterable

from minio.error import S3Error

from ingestion.common.exceptions import StorageError
from ingestion.common.logger import get_logger
from ingestion.common.storage import MinIOBronzeStorage


logger = get_logger(__name__)


DATASET_HOURLY = "weather_hourly"
DATASET_15MIN = "weather_15min"


def _to_utc(
    value: datetime,
) -> datetime:
    """
    Normalize a datetime to UTC.
    """

    if value.tzinfo is None:
        return value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    )


def _parse_datetime(
    value: str,
) -> datetime:
    """
    Parse an ISO datetime and normalize it to UTC.
    """

    normalized = value.replace(
        "Z",
        "+00:00",
    )

    parsed = datetime.fromisoformat(
        normalized
    )

    return _to_utc(
        parsed
    )


def _build_time_axis(
    *,
    start: datetime,
    end: datetime,
    step_minutes: int,
) -> list[str]:
    """
    Build the exact Open-Meteo UTC time axis
    expected for a requested interval.
    """

    start = (
        _to_utc(start)
        .replace(
            second=0,
            microsecond=0,
        )
    )

    end = (
        _to_utc(end)
        .replace(
            second=0,
            microsecond=0,
        )
    )

    if start > end:
        raise ValueError(
            "Requested start is after "
            "requested end."
        )

    axis = []

    current = start

    while current <= end:
        axis.append(
            current.strftime(
                "%Y-%m-%dT%H:%M"
            )
        )

        current += timedelta(
            minutes=step_minutes
        )

    return axis


def _expected_hourly_axis(
    *,
    requested_start_date: str,
    requested_end_date: str,
) -> list[str]:
    """
    Build the expected hourly axis.

    Historical requests use YYYY-MM-DD.
    Incremental requests use ISO datetimes.
    """

    try:
        historical_start = date.fromisoformat(
            requested_start_date
        )

        historical_end = date.fromisoformat(
            requested_end_date
        )

        start_datetime = datetime.combine(
            historical_start,
            time.min,
            tzinfo=timezone.utc,
        )

        end_datetime = datetime.combine(
            historical_end,
            time.max,
            tzinfo=timezone.utc,
        )

    except ValueError:
        start_datetime = _parse_datetime(
            requested_start_date
        )

        end_datetime = _parse_datetime(
            requested_end_date
        )

    return _build_time_axis(
        start=start_datetime,
        end=end_datetime,
        step_minutes=60,
    )


def _expected_15min_axis(
    *,
    requested_start_date: str,
    requested_end_date: str,
) -> list[str]:
    """
    Build the expected 15-minute axis.
    """

    start_datetime = _parse_datetime(
        requested_start_date
    )

    end_datetime = _parse_datetime(
        requested_end_date
    )

    return _build_time_axis(
        start=start_datetime,
        end=end_datetime,
        step_minutes=15,
    )


def _expected_axis(
    *,
    dataset: str,
    requested_start_date: str,
    requested_end_date: str,
) -> tuple[str, list[str]]:
    """
    Return the Open-Meteo response section and
    exact expected temporal axis.
    """

    if dataset == DATASET_HOURLY:
        return (
            "hourly",
            _expected_hourly_axis(
                requested_start_date=(
                    requested_start_date
                ),
                requested_end_date=(
                    requested_end_date
                ),
            ),
        )

    if dataset == DATASET_15MIN:
        return (
            "minutely_15",
            _expected_15min_axis(
                requested_start_date=(
                    requested_start_date
                ),
                requested_end_date=(
                    requested_end_date
                ),
            ),
        )

    raise ValueError(
        "Unsupported Open-Meteo dataset "
        f"for Bronze state validation: "
        f"{dataset}"
    )


def _has_complete_temporal_coverage(
    *,
    payload: dict,
    section_name: str,
    expected_axis: list[str],
) -> bool:
    """
    Validate the exact temporal coverage of a
    persisted Open-Meteo Bronze object.

    Metric values may legitimately contain nulls.
    The temporal axis itself may not be incomplete.
    """

    data = payload.get(
        "data"
    )

    if not isinstance(
        data,
        dict,
    ):
        return False

    section = data.get(
        section_name
    )

    if not isinstance(
        section,
        dict,
    ):
        return False

    actual_axis = section.get(
        "time"
    )

    if not isinstance(
        actual_axis,
        list,
    ):
        return False

    return (
        actual_axis
        == expected_axis
    )


def find_completed_location_ids(
    *,
    storage: MinIOBronzeStorage,
    source: str,
    dataset: str,
    requested_start_date: str,
    requested_end_date: str,
    id_fields: Iterable[str],
    ingestion_mode: str | None = None,
) -> set[str]:
    """
    Return location identifiers with a fully valid
    Bronze object for the exact requested interval.

    A location counts as complete only if:
        - source matches;
        - dataset matches;
        - requested start/end match exactly;
        - ingestion_mode matches when requested;
        - one of id_fields is present;
        - Bronze wrapper is structurally valid;
        - the Open-Meteo temporal axis exactly
          matches the complete expected interval.

    An incomplete temporal response is NOT reused:
    the location remains pending and will be
    downloaded again.

    Invalid/unreadable JSON fails explicitly.
    """

    section_name, expected_axis = (
        _expected_axis(
            dataset=dataset,
            requested_start_date=(
                requested_start_date
            ),
            requested_end_date=(
                requested_end_date
            ),
        )
    )

    prefix = (
        f"bronze/{source}/{dataset}/"
    )

    completed: set[str] = set()

    matching_candidates = 0
    incomplete_candidates = 0

    try:
        objects = (
            storage.client.list_objects(
                storage.bucket,
                prefix=prefix,
                recursive=True,
            )
        )

        for obj in objects:
            object_name = (
                obj.object_name
            )

            if not object_name.endswith(
                ".json"
            ):
                continue

            response = None

            try:
                response = (
                    storage.client.get_object(
                        storage.bucket,
                        object_name,
                    )
                )

                raw_data = (
                    response
                    .read()
                    .decode("utf-8")
                )

                payload = json.loads(
                    raw_data
                )

            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                OSError,
                ValueError,
            ) as exc:
                raise StorageError(
                    "Invalid Bronze JSON while "
                    "checking Open-Meteo state: "
                    f"{object_name}: {exc}"
                ) from exc

            finally:
                if response is not None:
                    response.close()
                    response.release_conn()

            if not isinstance(
                payload,
                dict,
            ):
                raise StorageError(
                    "Invalid Bronze wrapper: "
                    f"{object_name}"
                )

            metadata = payload.get(
                "metadata"
            )

            if not isinstance(
                metadata,
                dict,
            ):
                raise StorageError(
                    "Bronze object without valid "
                    f"metadata: {object_name}"
                )

            if (
                metadata.get("source")
                != source
            ):
                continue

            if (
                metadata.get("dataset")
                != dataset
            ):
                continue

            if (
                metadata.get(
                    "requested_start_date"
                )
                != requested_start_date
            ):
                continue

            if (
                metadata.get(
                    "requested_end_date"
                )
                != requested_end_date
            ):
                continue

            if (
                ingestion_mode is not None
                and metadata.get(
                    "ingestion_mode"
                )
                != ingestion_mode
            ):
                continue

            location_id = None

            for field in id_fields:
                value = metadata.get(
                    field
                )

                if value is not None:
                    location_id = str(
                        value
                    )
                    break

            if location_id is None:
                continue

            matching_candidates += 1

            if not (
                _has_complete_temporal_coverage(
                    payload=payload,
                    section_name=(
                        section_name
                    ),
                    expected_axis=(
                        expected_axis
                    ),
                )
            ):
                incomplete_candidates += 1

                logger.warning(
                    "%s Bronze object is "
                    "temporally incomplete and "
                    "will be downloaded again: "
                    "location_id=%s object=%s",
                    dataset,
                    location_id,
                    object_name,
                )

                continue

            completed.add(
                location_id
            )

    except S3Error as exc:
        raise StorageError(
            "Could not inspect Open-Meteo "
            f"Bronze state: {exc}"
        ) from exc

    logger.info(
        "Open-Meteo Bronze state: "
        "dataset=%s candidates=%d "
        "complete=%d incomplete=%d "
        "expected_points=%d",
        dataset,
        matching_candidates,
        len(completed),
        incomplete_candidates,
        len(expected_axis),
    )

    return completed