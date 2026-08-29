"""
Batch Open-Meteo ingestion.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone as dt_timezone
from typing import Any

from ingestion.common.config import (
    OPEN_METEO_BASE_URL,
)
from ingestion.common.http_client import HTTPClient
from ingestion.common.storage import MinIOBronzeStorage
from ingestion.open_meteo.ingest import (
    DEFAULT_HOURLY_VARIABLES,
    DEFAULT_MINUTELY_15_VARIABLES,
)


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
    Multi-coordinate Open-Meteo ingestion.

    Batch size 100 was validated against the
    real Open-Meteo API.
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
    ) -> None:
        if batch_size <= 0:
            raise ValueError(
                "batch_size must be greater than zero."
            )

        self.http_client = (
            http_client or HTTPClient()
        )
        self.storage = (
            storage or MinIOBronzeStorage()
        )
        self.batch_size = batch_size

    def _batches(
        self,
        locations: list[dict[str, Any]],
    ):
        for start in range(
            0,
            len(locations),
            self.batch_size,
        ):
            yield locations[
                start:start + self.batch_size
            ]

    @staticmethod
    def _coordinates(
        locations,
    ):
        latitude = ",".join(
            str(location["latitude"])
            for location in locations
        )

        longitude = ",".join(
            str(location["longitude"])
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
            and isinstance(response, dict)
        ):
            response = [response]

        if not isinstance(response, list):
            raise RuntimeError(
                "Expected Open-Meteo "
                "multi-location list response."
            )

        if len(response) != expected_count:
            raise RuntimeError(
                "Open-Meteo location count mismatch: "
                f"expected={expected_count}, "
                f"received={len(response)}"
            )

        return response

    def ingest_hourly_locations(
        self,
        *,
        locations,
        target_hour: datetime,
    ):
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

        paths = []

        for batch in self._batches(
            locations
        ):
            latitude, longitude = (
                self._coordinates(batch)
            )

            response = self.http_client.get_json(
                OPEN_METEO_BASE_URL,
                params={
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

            results = self._normalize_response(
                response,
                len(batch),
            )

            for location, data in zip(
                batch,
                results,
                strict=True,
            ):
                paths.append(
                    self.storage.save_json(
                        data,
                        source=self.SOURCE,
                        dataset=self.DATASET_HOURLY,
                        ingestion_mode="incremental",
                        requested_start_date=(
                            target_hour.isoformat()
                        ),
                        requested_end_date=(
                            target_hour.isoformat()
                        ),
                        extra_metadata={
                            "station_id": location[
                                "station_id"
                            ],
                            "station_name": location[
                                "station_name"
                            ],
                            "province": location[
                                "province"
                            ],
                            "latitude": location[
                                "latitude"
                            ],
                            "longitude": location[
                                "longitude"
                            ],
                        },
                    )
                )

        return paths

    def ingest_15min_locations(
        self,
        *,
        locations,
        start_datetime: datetime,
        end_datetime: datetime,
    ):
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

        start_text = start_datetime.strftime(
            "%Y-%m-%dT%H:%M"
        )
        end_text = end_datetime.strftime(
            "%Y-%m-%dT%H:%M"
        )

        paths = []

        for batch in self._batches(
            locations
        ):
            latitude, longitude = (
                self._coordinates(batch)
            )

            response = self.http_client.get_json(
                OPEN_METEO_BASE_URL,
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "minutely_15": ",".join(
                        DEFAULT_MINUTELY_15_VARIABLES
                    ),
                    "start_minutely_15": start_text,
                    "end_minutely_15": end_text,
                    "timezone": "UTC",
                },
            )

            results = self._normalize_response(
                response,
                len(batch),
            )

            for location, data in zip(
                batch,
                results,
                strict=True,
            ):
                paths.append(
                    self.storage.save_json(
                        data,
                        source=self.SOURCE,
                        dataset=self.DATASET_15MIN,
                        ingestion_mode="incremental",
                        requested_start_date=(
                            start_datetime.isoformat()
                        ),
                        requested_end_date=(
                            end_datetime.isoformat()
                        ),
                        extra_metadata={
                            "location_id": location[
                                "station_id"
                            ],
                            "station_name": location[
                                "station_name"
                            ],
                            "province": location[
                                "province"
                            ],
                            "latitude": location[
                                "latitude"
                            ],
                            "longitude": location[
                                "longitude"
                            ],
                        },
                    )
                )

        return paths
