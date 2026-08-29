"""
Open-Meteo ingestion.
"""

from __future__ import annotations

import time

from datetime import (
    datetime,
    timedelta,
)
from datetime import timezone as dt_timezone
from typing import Any

from ingestion.common.config import (
    OPEN_METEO_ARCHIVE_URL,
    OPEN_METEO_BASE_URL,
    OPEN_METEO_BATCH_DELAY_SECONDS,
    OPEN_METEO_HISTORICAL_FORECAST_URL,
    OPEN_METEO_MAX_RETRIES,
)
from ingestion.common.http_client import HTTPClient
from ingestion.common.logger import get_logger
from ingestion.common.storage import (
    MinIOBronzeStorage,
)
from ingestion.open_meteo.bronze_state import (
    find_completed_location_ids,
)
from ingestion.open_meteo.ingest import (
    DEFAULT_HOURLY_VARIABLES,
    DEFAULT_MINUTELY_15_VARIABLES,
)


logger = get_logger(__name__)


DEFAULT_BATCH_SIZE = 100


def _to_utc(
    value: datetime,
) -> datetime:
    if value.tzinfo is None:
        return value.replace(
            tzinfo=dt_timezone.utc
        )

    return value.astimezone(
        dt_timezone.utc
    )


class OpenMeteoBatchIngestion:
    """
    Robust multi-coordinate Open-Meteo ingestion.

    Guarantees:
        - controlled batching;
        - pacing between API calls;
        - transport retries;
        - resumable Bronze ingestion;
        - duplicate expected station protection;
        - exact response-location count;
        - exact expected temporal axis;
        - fail-closed completion validation.
    """

    SOURCE = "open_meteo"
    DATASET_HOURLY = "weather_hourly"
    DATASET_15MIN = "weather_15min"

    def __init__(
        self,
        *,
        http_client: HTTPClient | None = None,
        storage: MinIOBronzeStorage | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_delay_seconds: float = (
            OPEN_METEO_BATCH_DELAY_SECONDS
        ),
    ) -> None:
        if batch_size <= 0:
            raise ValueError(
                "batch_size must be greater "
                "than zero."
            )

        if batch_delay_seconds < 0:
            raise ValueError(
                "batch_delay_seconds cannot "
                "be negative."
            )

        self.http_client = (
            http_client
            or HTTPClient(
                max_retries=(
                    OPEN_METEO_MAX_RETRIES
                )
            )
        )

        self.storage = (
            storage
            or MinIOBronzeStorage()
        )

        self.batch_size = batch_size
        self.batch_delay_seconds = (
            batch_delay_seconds
        )

    def _batches(
        self,
        locations: list[
            dict[str, Any]
        ],
    ):
        for start in range(
            0,
            len(locations),
            self.batch_size,
        ):
            yield locations[
                start:
                start + self.batch_size
            ]

    def _paced_batches(
        self,
        locations,
    ):
        for index, batch in enumerate(
            self._batches(
                locations
            )
        ):
            if (
                index > 0
                and self.batch_delay_seconds
                > 0
            ):
                logger.info(
                    "Open-Meteo pacing: "
                    "waiting %.1f seconds "
                    "before next batch.",
                    self.batch_delay_seconds,
                )

                time.sleep(
                    self.batch_delay_seconds
                )

            yield batch

    @staticmethod
    def _coordinates(
        locations,
    ):
        latitude = ",".join(
            str(
                location["latitude"]
            )
            for location in locations
        )

        longitude = ",".join(
            str(
                location["longitude"]
            )
            for location in locations
        )

        return latitude, longitude

    @staticmethod
    def _normalize_response(
        response,
        expected_count,
    ):
        if (
            expected_count == 1
            and isinstance(
                response,
                dict,
            )
        ):
            response = [
                response
            ]

        if not isinstance(
            response,
            list,
        ):
            raise RuntimeError(
                "Expected Open-Meteo "
                "multi-location list response."
            )

        if (
            len(response)
            != expected_count
        ):
            raise RuntimeError(
                "Open-Meteo location count "
                "mismatch: "
                f"expected={expected_count}, "
                f"received={len(response)}"
            )

        return response

    @staticmethod
    def _expected_location_ids(
        locations,
    ) -> set[str]:
        ids = [
            str(
                location["station_id"]
            )
            for location in locations
        ]

        if len(ids) != len(
            set(ids)
        ):
            raise RuntimeError(
                "Duplicate station_id found "
                "in Open-Meteo location master."
            )

        return set(ids)

    def _existing_location_ids(
        self,
        *,
        dataset: str,
        requested_start_date: str,
        requested_end_date: str,
        ingestion_mode: str,
        id_fields,
        resume: bool,
    ) -> set[str]:
        if not resume:
            return set()

        if not isinstance(
            self.storage,
            MinIOBronzeStorage,
        ):
            logger.info(
                "Open-Meteo resumability "
                "disabled for injected "
                "non-MinIO storage."
            )
            return set()

        return (
            find_completed_location_ids(
                storage=self.storage,
                source=self.SOURCE,
                dataset=dataset,
                requested_start_date=(
                    requested_start_date
                ),
                requested_end_date=(
                    requested_end_date
                ),
                ingestion_mode=(
                    ingestion_mode
                ),
                id_fields=id_fields,
            )
        )

    @staticmethod
    def _pending_locations(
        *,
        locations,
        completed_ids,
    ):
        return [
            location
            for location in locations
            if str(
                location["station_id"]
            )
            not in completed_ids
        ]

    @staticmethod
    def _validate_complete(
        *,
        expected_ids: set[str],
        completed_ids: set[str],
        dataset: str,
    ) -> None:
        missing = (
            expected_ids
            - completed_ids
        )

        if missing:
            sample = sorted(
                missing
            )[:20]

            raise RuntimeError(
                f"{dataset} incomplete: "
                f"expected="
                f"{len(expected_ids)}, "
                f"completed="
                f"{len(completed_ids)}, "
                f"missing="
                f"{len(missing)}, "
                f"sample={sample}"
            )

        logger.info(
            "%s COMPLETE: %d/%d locations.",
            dataset,
            len(completed_ids),
            len(expected_ids),
        )

    @staticmethod
    def _build_time_axis(
        *,
        start: datetime,
        end: datetime,
        step_minutes: int,
    ) -> list[str]:
        start = _to_utc(
            start
        ).replace(
            second=0,
            microsecond=0,
        )

        end = _to_utc(
            end
        ).replace(
            second=0,
            microsecond=0,
        )

        current = start

        axis = []

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

    @staticmethod
    def _validate_time_axis(
        *,
        data,
        section_name: str,
        expected_axis: list[str],
        dataset: str,
        station_id: str,
    ) -> None:
        if not isinstance(
            data,
            dict,
        ):
            raise RuntimeError(
                f"{dataset} station "
                f"{station_id}: response "
                "is not a JSON object."
            )

        section = data.get(
            section_name
        )

        if not isinstance(
            section,
            dict,
        ):
            raise RuntimeError(
                f"{dataset} station "
                f"{station_id}: missing "
                f"'{section_name}' section."
            )

        actual_axis = section.get(
            "time"
        )

        if not isinstance(
            actual_axis,
            list,
        ):
            raise RuntimeError(
                f"{dataset} station "
                f"{station_id}: invalid "
                "time axis."
            )

        if actual_axis != expected_axis:
            raise RuntimeError(
                f"{dataset} station "
                f"{station_id}: temporal "
                "coverage mismatch. "
                f"expected_points="
                f"{len(expected_axis)}, "
                f"received_points="
                f"{len(actual_axis)}"
            )

    def ingest_hourly_locations(
        self,
        *,
        locations,
        target_hour: datetime,
        resume: bool = False,
    ):
        if not locations:
            raise ValueError(
                "No Open-Meteo locations "
                "provided."
            )

        target_hour = (
            _to_utc(
                target_hour
            )
            .replace(
                minute=0,
                second=0,
                microsecond=0,
            )
        )

        timestamp = (
            target_hour.strftime(
                "%Y-%m-%dT%H:%M"
            )
        )

        requested = (
            target_hour.isoformat()
        )

        expected_ids = (
            self._expected_location_ids(
                locations
            )
        )

        completed_ids = (
            self._existing_location_ids(
                dataset=(
                    self.DATASET_HOURLY
                ),
                requested_start_date=(
                    requested
                ),
                requested_end_date=(
                    requested
                ),
                ingestion_mode=(
                    "incremental"
                ),
                id_fields=(
                    "station_id",
                ),
                resume=resume,
            )
        )

        completed_ids &= expected_ids

        pending = (
            self._pending_locations(
                locations=locations,
                completed_ids=(
                    completed_ids
                ),
            )
        )

        logger.info(
            "Open-Meteo hourly: "
            "expected=%d existing=%d "
            "pending=%d",
            len(expected_ids),
            len(completed_ids),
            len(pending),
        )

        expected_axis = [
            timestamp
        ]

        paths = []

        for batch in (
            self._paced_batches(
                pending
            )
        ):
            latitude, longitude = (
                self._coordinates(
                    batch
                )
            )

            response = (
                self.http_client.get_json(
                    OPEN_METEO_BASE_URL,
                    params={
                        "latitude": (
                            latitude
                        ),
                        "longitude": (
                            longitude
                        ),
                        "hourly": ",".join(
                            DEFAULT_HOURLY_VARIABLES
                        ),
                        "start_hour": (
                            timestamp
                        ),
                        "end_hour": (
                            timestamp
                        ),
                        "timezone": "UTC",
                    },
                )
            )

            results = (
                self._normalize_response(
                    response,
                    len(batch),
                )
            )

            for location, data in zip(
                batch,
                results,
                strict=True,
            ):
                station_id = str(
                    location[
                        "station_id"
                    ]
                )

                self._validate_time_axis(
                    data=data,
                    section_name=(
                        "hourly"
                    ),
                    expected_axis=(
                        expected_axis
                    ),
                    dataset=(
                        self.DATASET_HOURLY
                    ),
                    station_id=(
                        station_id
                    ),
                )

                paths.append(
                    self.storage.save_json(
                        data,
                        source=self.SOURCE,
                        dataset=(
                            self.DATASET_HOURLY
                        ),
                        ingestion_mode=(
                            "incremental"
                        ),
                        requested_start_date=(
                            requested
                        ),
                        requested_end_date=(
                            requested
                        ),
                        extra_metadata={
                            "station_id": (
                                location[
                                    "station_id"
                                ]
                            ),
                            "station_name": (
                                location[
                                    "station_name"
                                ]
                            ),
                            "province": (
                                location[
                                    "province"
                                ]
                            ),
                            "latitude": (
                                location[
                                    "latitude"
                                ]
                            ),
                            "longitude": (
                                location[
                                    "longitude"
                                ]
                            ),
                        },
                    )
                )

                completed_ids.add(
                    station_id
                )

        self._validate_complete(
            expected_ids=expected_ids,
            completed_ids=completed_ids,
            dataset=(
                self.DATASET_HOURLY
            ),
        )

        return paths

    def ingest_hourly_range_locations(
        self,
        *,
        locations,
        start_date,
        end_date,
        resume: bool = False,
    ):
        if not locations:
            raise ValueError(
                "No Open-Meteo locations "
                "provided."
            )

        if start_date > end_date:
            raise ValueError(
                "start_date is after "
                "end_date."
            )

        requested_start = (
            start_date.isoformat()
        )
        requested_end = (
            end_date.isoformat()
        )

        expected_ids = (
            self._expected_location_ids(
                locations
            )
        )

        completed_ids = (
            self._existing_location_ids(
                dataset=(
                    self.DATASET_HOURLY
                ),
                requested_start_date=(
                    requested_start
                ),
                requested_end_date=(
                    requested_end
                ),
                ingestion_mode=(
                    "historical"
                ),
                id_fields=(
                    "station_id",
                ),
                resume=resume,
            )
        )

        completed_ids &= expected_ids

        pending = (
            self._pending_locations(
                locations=locations,
                completed_ids=(
                    completed_ids
                ),
            )
        )

        logger.info(
            "Open-Meteo historical hourly: "
            "expected=%d existing=%d "
            "pending=%d",
            len(expected_ids),
            len(completed_ids),
            len(pending),
        )

        start_datetime = datetime.combine(
            start_date,
            datetime.min.time(),
            tzinfo=dt_timezone.utc,
        )

        end_datetime = datetime.combine(
            end_date,
            datetime.max.time(),
            tzinfo=dt_timezone.utc,
        )

        expected_axis = (
            self._build_time_axis(
                start=start_datetime,
                end=end_datetime,
                step_minutes=60,
            )
        )

        paths = []

        for batch in (
            self._paced_batches(
                pending
            )
        ):
            latitude, longitude = (
                self._coordinates(
                    batch
                )
            )

            response = (
                self.http_client.get_json(
                    OPEN_METEO_ARCHIVE_URL,
                    params={
                        "latitude": (
                            latitude
                        ),
                        "longitude": (
                            longitude
                        ),
                        "hourly": ",".join(
                            DEFAULT_HOURLY_VARIABLES
                        ),
                        "start_date": (
                            requested_start
                        ),
                        "end_date": (
                            requested_end
                        ),
                        "timezone": "UTC",
                    },
                )
            )

            results = (
                self._normalize_response(
                    response,
                    len(batch),
                )
            )

            for location, data in zip(
                batch,
                results,
                strict=True,
            ):
                station_id = str(
                    location[
                        "station_id"
                    ]
                )

                self._validate_time_axis(
                    data=data,
                    section_name=(
                        "hourly"
                    ),
                    expected_axis=(
                        expected_axis
                    ),
                    dataset=(
                        self.DATASET_HOURLY
                    ),
                    station_id=(
                        station_id
                    ),
                )

                paths.append(
                    self.storage.save_json(
                        data,
                        source=self.SOURCE,
                        dataset=(
                            self.DATASET_HOURLY
                        ),
                        ingestion_mode=(
                            "historical"
                        ),
                        requested_start_date=(
                            requested_start
                        ),
                        requested_end_date=(
                            requested_end
                        ),
                        extra_metadata={
                            "station_id": (
                                location[
                                    "station_id"
                                ]
                            ),
                            "station_name": (
                                location[
                                    "station_name"
                                ]
                            ),
                            "province": (
                                location[
                                    "province"
                                ]
                            ),
                            "latitude": (
                                location[
                                    "latitude"
                                ]
                            ),
                            "longitude": (
                                location[
                                    "longitude"
                                ]
                            ),
                        },
                    )
                )

                completed_ids.add(
                    station_id
                )

        self._validate_complete(
            expected_ids=expected_ids,
            completed_ids=completed_ids,
            dataset=(
                self.DATASET_HOURLY
            ),
        )

        return paths

    def ingest_15min_locations(
        self,
        *,
        locations,
        start_datetime: datetime,
        end_datetime: datetime,
        resume: bool = False,
        ingestion_mode: str = (
            "incremental"
        ),
    ):
        if not locations:
            raise ValueError(
                "No Open-Meteo locations "
                "provided."
            )

        start_datetime = _to_utc(
            start_datetime
        )

        end_datetime = _to_utc(
            end_datetime
        )

        if (
            start_datetime
            > end_datetime
        ):
            raise ValueError(
                "start_datetime is after "
                "end_datetime."
            )

        if ingestion_mode not in {
            "incremental",
            "historical",
        }:
            raise ValueError(
                "Unsupported Open-Meteo "
                "ingestion_mode: "
                f"{ingestion_mode}"
            )

        if ingestion_mode == "historical":
            api_url = (
                OPEN_METEO_HISTORICAL_FORECAST_URL
            )
        else:
            api_url = OPEN_METEO_BASE_URL

        start_text = (
            start_datetime.strftime(
                "%Y-%m-%dT%H:%M"
            )
        )

        end_text = (
            end_datetime.strftime(
                "%Y-%m-%dT%H:%M"
            )
        )

        requested_start = (
            start_datetime.isoformat()
        )

        requested_end = (
            end_datetime.isoformat()
        )

        expected_ids = (
            self._expected_location_ids(
                locations
            )
        )

        completed_ids = (
            self._existing_location_ids(
                dataset=(
                    self.DATASET_15MIN
                ),
                requested_start_date=(
                    requested_start
                ),
                requested_end_date=(
                    requested_end
                ),
                ingestion_mode=(
                    ingestion_mode
                ),
                id_fields=(
                    "location_id",
                    "station_id",
                ),
                resume=resume,
            )
        )

        completed_ids &= expected_ids

        pending = (
            self._pending_locations(
                locations=locations,
                completed_ids=(
                    completed_ids
                ),
            )
        )

        logger.info(
            "Open-Meteo 15min: "
            "expected=%d existing=%d "
            "pending=%d",
            len(expected_ids),
            len(completed_ids),
            len(pending),
        )

        expected_axis = (
            self._build_time_axis(
                start=start_datetime,
                end=end_datetime,
                step_minutes=15,
            )
        )

        paths = []

        for batch in (
            self._paced_batches(
                pending
            )
        ):
            latitude, longitude = (
                self._coordinates(
                    batch
                )
            )

            response = (
                self.http_client.get_json(
                    api_url,
                    params={
                        "latitude": (
                            latitude
                        ),
                        "longitude": (
                            longitude
                        ),
                        "minutely_15": ",".join(
                            DEFAULT_MINUTELY_15_VARIABLES
                        ),
                        "start_minutely_15": (
                            start_text
                        ),
                        "end_minutely_15": (
                            end_text
                        ),
                        "timezone": "UTC",
                    },
                )
            )

            results = (
                self._normalize_response(
                    response,
                    len(batch),
                )
            )

            for location, data in zip(
                batch,
                results,
                strict=True,
            ):
                station_id = str(
                    location[
                        "station_id"
                    ]
                )

                self._validate_time_axis(
                    data=data,
                    section_name=(
                        "minutely_15"
                    ),
                    expected_axis=(
                        expected_axis
                    ),
                    dataset=(
                        self.DATASET_15MIN
                    ),
                    station_id=(
                        station_id
                    ),
                )

                paths.append(
                    self.storage.save_json(
                        data,
                        source=self.SOURCE,
                        dataset=(
                            self.DATASET_15MIN
                        ),
                        ingestion_mode=(
                            ingestion_mode
                        ),
                        requested_start_date=(
                            requested_start
                        ),
                        requested_end_date=(
                            requested_end
                        ),
                        extra_metadata={
                            "location_id": (
                                location[
                                    "station_id"
                                ]
                            ),
                            "station_id": (
                                location[
                                    "station_id"
                                ]
                            ),
                            "station_name": (
                                location[
                                    "station_name"
                                ]
                            ),
                            "province": (
                                location[
                                    "province"
                                ]
                            ),
                            "latitude": (
                                location[
                                    "latitude"
                                ]
                            ),
                            "longitude": (
                                location[
                                    "longitude"
                                ]
                            ),
                        },
                    )
                )

                completed_ids.add(
                    station_id
                )

        self._validate_complete(
            expected_ids=expected_ids,
            completed_ids=completed_ids,
            dataset=(
                self.DATASET_15MIN
            ),
        )

        return paths