"""
Open-Meteo ingestion.
"""

from __future__ import annotations

import time

from copy import deepcopy

from datetime import (
    datetime,
    timedelta,
    timezone as dt_timezone,
)

from typing import Any

from ingestion.common.config import (
    OPEN_METEO_ARCHIVE_URL,
    OPEN_METEO_BASE_URL,
    OPEN_METEO_BATCH_DELAY_SECONDS,
    OPEN_METEO_HISTORICAL_FORECAST_URL,
    OPEN_METEO_MAX_RETRIES,
    OPEN_METEO_API_PARAMS,
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
        """
        Ingest one Open-Meteo hourly observation for all locations.

        Data is persisted in one canonical Bronze object per
        station and UTC observation day.

        Existing daily observations are preserved and merged
        by hourly.time.
        """

        if not locations:
            raise ValueError(
                "No Open-Meteo locations provided."
            )

        target_hour = (
            _to_utc(target_hour)
            .replace(
                minute=0,
                second=0,
                microsecond=0,
            )
        )

        timestamp = target_hour.strftime(
            "%Y-%m-%dT%H:%M"
        )

        requested = (
            target_hour.isoformat()
        )

        observation_date = (
            target_hour.date()
        )

        year = observation_date.strftime(
            "%Y"
        )
        month = observation_date.strftime(
            "%m"
        )
        day = observation_date.strftime(
            "%d"
        )

        expected_ids = (
            self._expected_location_ids(
                locations
            )
        )

        completed_ids: set[str] = set()

        existing_data_by_station = {}

        # ------------------------------------------------------------
        # Inspect canonical daily objects.
        #
        # Even when resume=False we read existing data because the
        # new hour must be merged without deleting previous hours.
        # ------------------------------------------------------------

        if isinstance(
                self.storage,
                MinIOBronzeStorage,
        ):
            for location in locations:
                station_id = str(
                    location["station_id"]
                )

                object_name = (
                    "bronze/open_meteo/"
                    f"{self.DATASET_HOURLY}/"
                    f"year={year}/"
                    f"month={month}/"
                    f"day={day}/"
                    f"station_id={station_id}.json"
                )

                if not self.storage.object_exists(
                        object_name
                ):
                    continue

                payload = self.storage.read_json(
                    object_name
                )

                if not isinstance(
                        payload,
                        dict,
                ):
                    raise RuntimeError(
                        "Invalid existing Open-Meteo "
                        "Bronze wrapper: "
                        f"{object_name}"
                    )

                existing_data = payload.get(
                    "data"
                )

                if not isinstance(
                        existing_data,
                        dict,
                ):
                    raise RuntimeError(
                        "Invalid existing Open-Meteo "
                        "Bronze data: "
                        f"{object_name}"
                    )

                hourly = existing_data.get(
                    "hourly"
                )

                if not isinstance(
                        hourly,
                        dict,
                ):
                    raise RuntimeError(
                        "Invalid existing Open-Meteo "
                        "hourly section: "
                        f"{object_name}"
                    )

                existing_axis = hourly.get(
                    "time"
                )

                if not isinstance(
                        existing_axis,
                        list,
                ):
                    raise RuntimeError(
                        "Invalid existing Open-Meteo "
                        "hourly time axis: "
                        f"{object_name}"
                    )

                existing_data_by_station[
                    station_id
                ] = existing_data

                if (
                        resume
                        and timestamp in existing_axis
                ):
                    completed_ids.add(
                        station_id
                    )

        elif resume:
            logger.info(
                "Open-Meteo resumability "
                "disabled for injected "
                "non-MinIO storage."
            )

        pending = (
            self._pending_locations(
                locations=locations,
                completed_ids=completed_ids,
            )
        )

        logger.info(
            "Open-Meteo hourly: "
            "timestamp=%s expected=%d "
            "existing=%d pending=%d",
            timestamp,
            len(expected_ids),
            len(completed_ids),
            len(pending),
        )

        expected_axis = [
            timestamp
        ]

        paths = []

        for batch in self._paced_batches(
                pending
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
                        **OPEN_METEO_API_PARAMS,
                        "latitude": latitude,
                        "longitude": longitude,
                        "hourly": ",".join(
                            DEFAULT_HOURLY_VARIABLES
                        ),
                        "start_hour": timestamp,
                        "end_hour": timestamp,
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
                    location["station_id"]
                )

                self._validate_time_axis(
                    data=data,
                    section_name="hourly",
                    expected_axis=expected_axis,
                    dataset=self.DATASET_HOURLY,
                    station_id=station_id,
                )

                object_name = (
                    "bronze/open_meteo/"
                    f"{self.DATASET_HOURLY}/"
                    f"year={year}/"
                    f"month={month}/"
                    f"day={day}/"
                    f"station_id={station_id}.json"
                )

                existing_data = (
                    existing_data_by_station.get(
                        station_id
                    )
                )

                # --------------------------------------------------------
                # Merge existing + new hourly values by timestamp.
                # New API values prevail when the timestamp already exists.
                # --------------------------------------------------------

                if existing_data is None:
                    merged_data = deepcopy(
                        data
                    )

                else:
                    existing_hourly = (
                        existing_data["hourly"]
                    )

                    new_hourly = (
                        data["hourly"]
                    )

                    existing_times = (
                        existing_hourly["time"]
                    )

                    new_times = (
                        new_hourly["time"]
                    )

                    fields = (
                            set(existing_hourly)
                            | set(new_hourly)
                    )

                    fields.discard(
                        "time"
                    )

                    rows = {}

                    for source_hourly, times in (
                            (
                                    existing_hourly,
                                    existing_times,
                            ),
                            (
                                    new_hourly,
                                    new_times,
                            ),
                    ):
                        for field in fields:
                            values = (
                                source_hourly.get(
                                    field
                                )
                            )

                            if values is None:
                                continue

                            if not isinstance(
                                    values,
                                    list,
                            ):
                                raise RuntimeError(
                                    "Invalid Open-Meteo "
                                    "hourly field: "
                                    f"{field}"
                                )

                            if len(values) != len(
                                    times
                            ):
                                raise RuntimeError(
                                    "Open-Meteo hourly "
                                    "field length mismatch: "
                                    f"{field}"
                                )

                        for index, time_value in enumerate(
                                times
                        ):
                            row = rows.setdefault(
                                time_value,
                                {}
                            )

                            for field in fields:
                                values = (
                                    source_hourly.get(
                                        field
                                    )
                                )

                                if values is not None:
                                    row[field] = (
                                        values[index]
                                    )

                    merged_times = sorted(
                        rows
                    )

                    merged_hourly = {
                        "time": merged_times,
                    }

                    for field in sorted(
                            fields
                    ):
                        merged_hourly[field] = [
                            rows[time_value].get(
                                field
                            )
                            for time_value
                            in merged_times
                        ]

                    merged_data = deepcopy(
                        data
                    )

                    merged_data[
                        "hourly"
                    ] = merged_hourly

                paths.append(
                    self.storage.save_json(
                        merged_data,
                        source=self.SOURCE,
                        dataset=(
                            self.DATASET_HOURLY
                        ),
                        object_name=object_name,
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
                            "observation_date": (
                                observation_date
                                .isoformat()
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
            dataset=self.DATASET_HOURLY,
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
        """
        Ingest historical Open-Meteo hourly data using
        canonical station per observation-day Bronze objects.

        Canonical path:

            bronze/open_meteo/weather_hourly/
            year=YYYY/month=MM/day=DD/
            station_id=<station_id>.json

        When resume=True, only missing or temporally incomplete
        station/day objects are downloaded again.
        """

        if not locations:
            raise ValueError(
                "No Open-Meteo locations provided."
            )

        if start_date > end_date:
            raise ValueError(
                "start_date is after end_date."
            )

        expected_ids = (
            self._expected_location_ids(
                locations
            )
        )

        paths = []

        current_date = start_date

        while current_date <= end_date:
            requested_date = (
                current_date.isoformat()
            )

            year = (
                current_date.strftime("%Y")
            )
            month = (
                current_date.strftime("%m")
            )
            day = (
                current_date.strftime("%d")
            )

            start_datetime = datetime.combine(
                current_date,
                datetime.min.time(),
                tzinfo=dt_timezone.utc,
            )

            end_datetime = datetime.combine(
                current_date,
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

            completed_ids: set[str] = set()

            # ------------------------------------------------------------
            # Canonical daily Bronze state
            # ------------------------------------------------------------

            if (
                    resume
                    and isinstance(
                self.storage,
                MinIOBronzeStorage,
            )
            ):
                for location in locations:
                    station_id = str(
                        location[
                            "station_id"
                        ]
                    )

                    object_name = (
                        "bronze/open_meteo/"
                        f"{self.DATASET_HOURLY}/"
                        f"year={year}/"
                        f"month={month}/"
                        f"day={day}/"
                        f"station_id={station_id}.json"
                    )

                    if not (
                            self.storage.object_exists(
                                object_name
                            )
                    ):
                        continue

                    payload = (
                        self.storage.read_json(
                            object_name
                        )
                    )

                    if not isinstance(
                            payload,
                            dict,
                    ):
                        logger.warning(
                            "Invalid Open-Meteo Bronze "
                            "wrapper will be downloaded "
                            "again: station_id=%s "
                            "date=%s object=%s",
                            station_id,
                            requested_date,
                            object_name,
                        )
                        continue

                    data = payload.get(
                        "data"
                    )

                    if not isinstance(
                            data,
                            dict,
                    ):
                        logger.warning(
                            "Invalid Open-Meteo Bronze "
                            "data will be downloaded "
                            "again: station_id=%s "
                            "date=%s object=%s",
                            station_id,
                            requested_date,
                            object_name,
                        )
                        continue

                    hourly = data.get(
                        "hourly"
                    )

                    if not isinstance(
                            hourly,
                            dict,
                    ):
                        logger.warning(
                            "Missing Open-Meteo hourly "
                            "section: station_id=%s "
                            "date=%s object=%s",
                            station_id,
                            requested_date,
                            object_name,
                        )
                        continue

                    actual_axis = hourly.get(
                        "time"
                    )

                    if (
                            actual_axis
                            != expected_axis
                    ):
                        logger.warning(
                            "Incomplete Open-Meteo hourly "
                            "Bronze object will be "
                            "downloaded again: "
                            "station_id=%s date=%s "
                            "actual_points=%s "
                            "expected_points=%s",
                            station_id,
                            requested_date,
                            (
                                len(actual_axis)
                                if isinstance(
                                    actual_axis,
                                    list,
                                )
                                else 0
                            ),
                            len(expected_axis),
                        )
                        continue

                    completed_ids.add(
                        station_id
                    )

            elif resume:
                logger.info(
                    "Open-Meteo resumability "
                    "disabled for injected "
                    "non-MinIO storage."
                )

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
                "date=%s expected=%d "
                "existing=%d pending=%d",
                requested_date,
                len(expected_ids),
                len(completed_ids),
                len(pending),
            )

            # ------------------------------------------------------------
            # Download only pending stations for this observation day
            # ------------------------------------------------------------

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
                            **OPEN_METEO_API_PARAMS,
                        "latitude": latitude,
                            "longitude": longitude,
                            "hourly": ",".join(
                                DEFAULT_HOURLY_VARIABLES
                            ),
                            "start_date": (
                                requested_date
                            ),
                            "end_date": (
                                requested_date
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
                        section_name="hourly",
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

                    object_name = (
                        "bronze/open_meteo/"
                        f"{self.DATASET_HOURLY}/"
                        f"year={year}/"
                        f"month={month}/"
                        f"day={day}/"
                        f"station_id={station_id}.json"
                    )

                    paths.append(
                        self.storage.save_json(
                            data,
                            source=self.SOURCE,
                            dataset=(
                                self.DATASET_HOURLY
                            ),
                            object_name=(
                                object_name
                            ),
                            ingestion_mode=(
                                "historical"
                            ),
                            requested_start_date=(
                                requested_date
                            ),
                            requested_end_date=(
                                requested_date
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
                                "observation_date": (
                                    requested_date
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

            current_date += timedelta(
                days=1
            )

        return paths

    def ingest_15min_locations(
            self,
            *,
            locations,
            start_datetime: datetime,
            end_datetime: datetime,
            resume: bool = False,
            ingestion_mode: str = "historical",
    ):
        """
        Reconstruct historical Open-Meteo 15-minute weather
        using canonical station per UTC observation-day objects.

        Canonical path:

            bronze/open_meteo/weather_15min/
            year=YYYY/month=MM/day=DD/
            station_id=<station_id>.json
        """

        if not locations:
            raise ValueError(
                "No Open-Meteo locations provided."
            )

        start_datetime = _to_utc(
            start_datetime
        )

        end_datetime = _to_utc(
            end_datetime
        )

        if start_datetime > end_datetime:
            raise ValueError(
                "start_datetime is after end_datetime."
            )

        if ingestion_mode != "historical":
            raise ValueError(
                "Open-Meteo 15-minute batch ingestion "
                "is reserved for historical reconstruction."
            )

        expected_ids = (
            self._expected_location_ids(
                locations
            )
        )

        paths = []

        current_date = (
            start_datetime.date()
        )

        final_date = (
            end_datetime.date()
        )

        while current_date <= final_date:
            requested_date = (
                current_date.isoformat()
            )

            year = current_date.strftime(
                "%Y"
            )
            month = current_date.strftime(
                "%m"
            )
            day = current_date.strftime(
                "%d"
            )

            day_start = datetime.combine(
                current_date,
                datetime.min.time(),
                tzinfo=dt_timezone.utc,
            )

            day_end = datetime.combine(
                current_date,
                datetime.max.time(),
                tzinfo=dt_timezone.utc,
            )

            expected_axis = (
                self._build_time_axis(
                    start=day_start,
                    end=day_end,
                    step_minutes=15,
                )
            )

            completed_ids: set[str] = set()

            # ------------------------------------------------------------
            # Canonical daily Bronze state
            # ------------------------------------------------------------

            if (
                    resume
                    and isinstance(
                self.storage,
                MinIOBronzeStorage,
            )
            ):
                for location in locations:
                    station_id = str(
                        location["station_id"]
                    )

                    object_name = (
                        "bronze/open_meteo/"
                        f"{self.DATASET_15MIN}/"
                        f"year={year}/"
                        f"month={month}/"
                        f"day={day}/"
                        f"station_id={station_id}.json"
                    )

                    if not self.storage.object_exists(
                            object_name
                    ):
                        continue

                    payload = self.storage.read_json(
                        object_name
                    )

                    if not isinstance(
                            payload,
                            dict,
                    ):
                        logger.warning(
                            "Invalid Open-Meteo 15min "
                            "Bronze wrapper will be "
                            "downloaded again: "
                            "station_id=%s date=%s",
                            station_id,
                            requested_date,
                        )
                        continue

                    data = payload.get(
                        "data"
                    )

                    if not isinstance(
                            data,
                            dict,
                    ):
                        continue

                    minutely = data.get(
                        "minutely_15"
                    )

                    if not isinstance(
                            minutely,
                            dict,
                    ):
                        continue

                    actual_axis = minutely.get(
                        "time"
                    )

                    if actual_axis != expected_axis:
                        logger.warning(
                            "Incomplete Open-Meteo 15min "
                            "Bronze object will be "
                            "downloaded again: "
                            "station_id=%s date=%s "
                            "actual_points=%s "
                            "expected_points=%s",
                            station_id,
                            requested_date,
                            (
                                len(actual_axis)
                                if isinstance(
                                    actual_axis,
                                    list,
                                )
                                else 0
                            ),
                            len(expected_axis),
                        )
                        continue

                    completed_ids.add(
                        station_id
                    )

            elif resume:
                logger.info(
                    "Open-Meteo resumability "
                    "disabled for injected "
                    "non-MinIO storage."
                )

            pending = (
                self._pending_locations(
                    locations=locations,
                    completed_ids=completed_ids,
                )
            )

            logger.info(
                "Open-Meteo historical 15min: "
                "date=%s expected=%d "
                "existing=%d pending=%d",
                requested_date,
                len(expected_ids),
                len(completed_ids),
                len(pending),
            )

            start_text = (
                day_start.strftime(
                    "%Y-%m-%dT%H:%M"
                )
            )

            end_text = (
                day_end.strftime(
                    "%Y-%m-%dT%H:%M"
                )
            )

            # ------------------------------------------------------------
            # Download only pending stations for this day
            # ------------------------------------------------------------

            for batch in self._paced_batches(
                    pending
            ):
                latitude, longitude = (
                    self._coordinates(
                        batch
                    )
                )

                response = (
                    self.http_client.get_json(
                        OPEN_METEO_HISTORICAL_FORECAST_URL,
                        params={
                            **OPEN_METEO_API_PARAMS,
                        "latitude": latitude,
                            "longitude": longitude,
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
                        location["station_id"]
                    )

                    self._validate_time_axis(
                        data=data,
                        section_name="minutely_15",
                        expected_axis=expected_axis,
                        dataset=(
                            self.DATASET_15MIN
                        ),
                        station_id=station_id,
                    )

                    object_name = (
                        "bronze/open_meteo/"
                        f"{self.DATASET_15MIN}/"
                        f"year={year}/"
                        f"month={month}/"
                        f"day={day}/"
                        f"station_id={station_id}.json"
                    )

                    paths.append(
                        self.storage.save_json(
                            data,
                            source=self.SOURCE,
                            dataset=(
                                self.DATASET_15MIN
                            ),
                            object_name=object_name,
                            ingestion_mode="historical",
                            requested_start_date=(
                                requested_date
                            ),
                            requested_end_date=(
                                requested_date
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
                                "observation_date": (
                                    requested_date
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
                dataset=self.DATASET_15MIN,
            )

            current_date += timedelta(
                days=1
            )

        return paths
