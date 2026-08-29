"""
Client for the AEMET OpenData API.
"""

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
