"""
Client for the AEMET OpenData API.
"""

from datetime import date
from typing import Any

from ingestion.common.config import AEMET_API_KEY, AEMET_BASE_URL
from ingestion.common.exceptions import (
    APIResponseError,
    ConfigurationError,
    InvalidDateRangeError,
)
from ingestion.common.http_client import HTTPClient
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


class AemetClient:
    """
    Client used to retrieve meteorological data from AEMET OpenData.
    """

    def __init__(
        self,
        api_key: str | None = AEMET_API_KEY,
        http_client: HTTPClient | None = None,
    ) -> None:
        if not api_key:
            raise ConfigurationError(
                "AEMET_API_KEY is required to use the AEMET connector."
            )

        self.api_key = api_key
        self.http_client = http_client or HTTPClient()

    @staticmethod
    def _validate_date_range(
        start_date: date,
        end_date: date,
    ) -> None:
        """Validate the requested date range."""

        if start_date > end_date:
            raise InvalidDateRangeError(
                f"Invalid date range: {start_date} is after {end_date}."
            )

    @staticmethod
    def _format_date(value: date) -> str:
        """Convert a date to the format required by AEMET OpenData."""

        return f"{value.isoformat()}T00:00:00UTC"

    def _get_aemet_data(
        self,
        endpoint: str,
    ) -> dict[str, Any] | list[Any]:
        """
        Execute the two-step AEMET OpenData request.

        The first request returns metadata containing the URL where
        the requested dataset can be downloaded.
        """

        headers = {
            "accept": "application/json",
            "api_key": self.api_key,
        }

        metadata = self.http_client.get_json(
            endpoint,
            headers=headers,
        )

        if not isinstance(metadata, dict):
            raise APIResponseError(
                "Unexpected AEMET metadata response format."
            )

        data_url = metadata.get("datos")

        if not data_url:
            raise APIResponseError(
                "AEMET response does not contain a data URL."
            )

        logger.info(
            "AEMET dataset URL obtained successfully."
        )

        return self.http_client.get_json(data_url)

    def get_daily_climatological_values(
        self,
        *,
        start_date: date,
        end_date: date,
        station_id: str,
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve daily climatological observations for an AEMET station.
        """

        self._validate_date_range(start_date, end_date)

        if not station_id.strip():
            raise ValueError(
                "A valid AEMET station identifier must be provided."
            )

        start = self._format_date(start_date)
        end = self._format_date(end_date)

        endpoint = (
            f"{AEMET_BASE_URL}/valores/climatologicos/diarios/datos/"
            f"fechaini/{start}/"
            f"fechafin/{end}/"
            f"estacion/{station_id}"
        )

        logger.info(
            "Requesting AEMET daily climatological values "
            "for station=%s, period=%s -> %s",
            station_id,
            start_date,
            end_date,
        )

        return self._get_aemet_data(endpoint)