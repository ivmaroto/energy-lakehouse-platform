"""
Client for the Open-Meteo API.
"""

from datetime import date
from typing import Any

from ingestion.common.config import (
    OPEN_METEO_ARCHIVE_URL,
    OPEN_METEO_BASE_URL,
)
from ingestion.common.exceptions import InvalidDateRangeError
from ingestion.common.http_client import HTTPClient
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


class OpenMeteoClient:
    """
    Client used to retrieve meteorological data from Open-Meteo.
    """

    def __init__(self, http_client: HTTPClient | None = None) -> None:
        self.http_client = http_client or HTTPClient()

    @staticmethod
    def _validate_coordinates(
        latitude: float,
        longitude: float,
    ) -> None:
        """Validate geographical coordinates."""

        if not -90 <= latitude <= 90:
            raise ValueError(
                f"Invalid latitude: {latitude}. "
                "Expected a value between -90 and 90."
            )

        if not -180 <= longitude <= 180:
            raise ValueError(
                f"Invalid longitude: {longitude}. "
                "Expected a value between -180 and 180."
            )

    @staticmethod
    def _validate_date_range(
        start_date: date,
        end_date: date,
    ) -> None:
        """Validate a historical date range."""

        if start_date > end_date:
            raise InvalidDateRangeError(
                f"Invalid date range: {start_date} is after {end_date}."
            )

    def get_historical_weather(
        self,
        *,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        hourly_variables: list[str],
        timezone: str = "UTC",
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve historical hourly weather data.

        Parameters
        ----------
        latitude:
            Latitude of the requested location.
        longitude:
            Longitude of the requested location.
        start_date:
            First date of the requested period.
        end_date:
            Last date of the requested period.
        hourly_variables:
            Open-Meteo hourly variables to retrieve.
        timezone:
            Timezone used by the API response.
        """

        self._validate_coordinates(latitude, longitude)
        self._validate_date_range(start_date, end_date)

        if not hourly_variables:
            raise ValueError(
                "At least one hourly weather variable must be provided."
            )

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(hourly_variables),
            "timezone": timezone,
        }

        logger.info(
            "Requesting Open-Meteo historical data: %s -> %s",
            start_date,
            end_date,
        )

        return self.http_client.get_json(
            OPEN_METEO_ARCHIVE_URL,
            params=params,
        )

    def get_current_weather(
        self,
        *,
        latitude: float,
        longitude: float,
        current_variables: list[str],
        timezone: str = "UTC",
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve current meteorological data.
        """

        self._validate_coordinates(latitude, longitude)

        if not current_variables:
            raise ValueError(
                "At least one current weather variable must be provided."
            )

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": ",".join(current_variables),
            "timezone": timezone,
        }

        logger.info(
            "Requesting current Open-Meteo data for "
            "latitude=%s longitude=%s",
            latitude,
            longitude,
        )

        return self.http_client.get_json(
            OPEN_METEO_BASE_URL,
            params=params,
        )