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

    @property
    def headers(self) -> dict[str, str]:
        """Return the HTTP headers required by AEMET."""

        return {
            "accept": "application/json",
            "api_key": self.api_key,
        }

    def _get_aemet_metadata(
        self,
        endpoint: str,
    ) -> dict[str, Any]:
        """
        Execute the first step of an AEMET OpenData request.

        AEMET normally returns metadata containing temporary URLs
        for the requested data and its metadata definition.
        """

        metadata = self.http_client.get_json(
            endpoint,
            headers=self.headers,
        )

        if not isinstance(metadata, dict):
            raise APIResponseError(
                "Unexpected AEMET metadata response format."
            )

        return metadata

    def _get_aemet_data(
        self,
        endpoint: str,
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve an AEMET dataset whose payload is JSON.
        """

        metadata = self._get_aemet_metadata(endpoint)

        data_url = metadata.get("datos")

        if not data_url:
            raise APIResponseError(
                "AEMET response does not contain a data URL."
            )

        logger.info(
            "AEMET dataset URL obtained successfully."
        )

        return self.http_client.get_json(data_url)

    def _get_aemet_text(
        self,
        endpoint: str,
    ) -> str:
        """
        Retrieve an AEMET dataset whose payload is plain text.

        This is required for datasets such as the special
        radiation network, which returns semicolon-separated text.
        """

        metadata = self._get_aemet_metadata(endpoint)

        data_url = metadata.get("datos")

        if not data_url:
            raise APIResponseError(
                "AEMET response does not contain a data URL."
            )

        logger.info(
            "AEMET text dataset URL obtained successfully."
        )

        response = self.http_client.get(data_url)

        return response.text

    def get_stations(
        self,
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve the AEMET climatological station inventory.
        """

        endpoint = (
            f"{AEMET_BASE_URL}/valores/climatologicos/"
            "inventarioestaciones/todasestaciones"
        )

        logger.info(
            "Requesting AEMET climatological station inventory."
        )

        return self._get_aemet_data(endpoint)

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

    def get_current_observations(
        self,
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve current conventional observations from all AEMET stations.
        """

        endpoint = (
            f"{AEMET_BASE_URL}/observacion/convencional/todas"
        )

        logger.info(
            "Requesting AEMET conventional observations."
        )

        return self._get_aemet_data(endpoint)

    def get_radiation_data(
        self,
    ) -> str:
        """
        Retrieve data from the AEMET special radiation network.

        The source dataset is returned as semicolon-separated plain text
        and is intentionally not parsed in the client layer.
        """

        endpoint = (
            f"{AEMET_BASE_URL}/red/especial/radiacion"
        )

        logger.info(
            "Requesting AEMET special radiation network data."
        )

        return self._get_aemet_text(endpoint)

    def get_radiation_metadata(
        self,
    ) -> str:
        """
        Retrieve metadata for the AEMET special radiation network.
        """

        endpoint = (
            f"{AEMET_BASE_URL}/red/especial/radiacion"
        )

        metadata = self._get_aemet_metadata(endpoint)

        metadata_url = metadata.get("metadatos")

        if not metadata_url:
            raise APIResponseError(
                "AEMET response does not contain a metadata URL."
            )

        logger.info(
            "Requesting AEMET radiation metadata."
        )

        response = self.http_client.get(metadata_url)

        return response.text