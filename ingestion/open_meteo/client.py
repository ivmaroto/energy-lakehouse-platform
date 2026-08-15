"""
Client for the Open-Meteo API.
"""

from datetime import date, datetime, timezone as dt_timezone
from typing import Any

from ingestion.common.config import (
    OPEN_METEO_ARCHIVE_URL,
    OPEN_METEO_BASE_URL,
    OPEN_METEO_HISTORICAL_FORECAST_URL,
)
from ingestion.common.exceptions import InvalidDateRangeError
from ingestion.common.http_client import HTTPClient
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


class OpenMeteoClient:
    """
    Client used to retrieve meteorological data from Open-Meteo.
    """

    def __init__(
        self,
        http_client: HTTPClient | None = None,
    ) -> None:
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

    @staticmethod
    def _validate_variables(
        variables: list[str],
        *,
        variable_type: str,
    ) -> None:
        """Validate that at least one variable has been provided."""

        if not variables:
            raise ValueError(
                f"At least one {variable_type} weather variable "
                "must be provided."
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
        Retrieve historical hourly weather data
        from the Open-Meteo Historical Weather API.
        """

        self._validate_coordinates(latitude, longitude)
        self._validate_date_range(start_date, end_date)
        self._validate_variables(
            hourly_variables,
            variable_type="hourly",
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
            "Requesting Open-Meteo historical weather data: %s -> %s",
            start_date,
            end_date,
        )

        return self.http_client.get_json(
            OPEN_METEO_ARCHIVE_URL,
            params=params,
        )

    def get_historical_forecast(
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
        Retrieve historical forecast data.

        This endpoint is used for variables that are not available
        through the Historical Weather API, such as the validated
        wind variables at 80 m and 120 m.
        """

        self._validate_coordinates(latitude, longitude)
        self._validate_date_range(start_date, end_date)
        self._validate_variables(
            hourly_variables,
            variable_type="historical forecast hourly",
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
            "Requesting Open-Meteo historical forecast data: %s -> %s",
            start_date,
            end_date,
        )

        return self.http_client.get_json(
            OPEN_METEO_HISTORICAL_FORECAST_URL,
            params=params,
        )

    def get_minutely_15_weather(
            self,
            *,
            latitude: float,
            longitude: float,
            start_datetime: datetime,
            end_datetime: datetime,
            minutely_15_variables: list[str],
            timezone: str = "UTC",
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve 15-minutely weather data from Open-Meteo
        for an exact temporal interval.
        """

        self._validate_coordinates(latitude, longitude)

        if start_datetime > end_datetime:
            raise InvalidDateRangeError(
                f"Invalid date range: "
                f"{start_datetime} is after {end_datetime}."
            )

        self._validate_variables(
            minutely_15_variables,
            variable_type="15-minutely",
        )

        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(
                tzinfo=dt_timezone.utc
            )

        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(
                tzinfo=dt_timezone.utc
            )

        start_datetime = start_datetime.astimezone(dt_timezone.utc)

        end_datetime = end_datetime.astimezone(dt_timezone.utc)

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_minutely_15": start_datetime.strftime(
                "%Y-%m-%dT%H:%M"
            ),
            "end_minutely_15": end_datetime.strftime(
                "%Y-%m-%dT%H:%M"
            ),
            "minutely_15": ",".join(
                minutely_15_variables
            ),
            "timezone": timezone,
        }

        logger.info(
            "Requesting Open-Meteo 15-minutely data: %s -> %s",
            start_datetime,
            end_datetime,
        )

        return self.http_client.get_json(
            OPEN_METEO_BASE_URL,
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
        self._validate_variables(
            current_variables,
            variable_type="current",
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