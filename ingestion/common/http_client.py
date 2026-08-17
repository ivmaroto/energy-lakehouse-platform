"""
Reusable HTTP client for the ingestion layer.
"""

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.common.config import HTTP_MAX_RETRIES, HTTP_TIMEOUT
from ingestion.common.exceptions import (
    APIAuthenticationError,
    APIConnectionError,
    APIRequestError,
    APIResponseError,
    EmptyResponseError,
)
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


class HTTPClient:
    """
    Common HTTP client used by the external API connectors.

    Provides timeout handling, automatic retries and common
    error handling.
    """

    def __init__(
        self,
        timeout: int = HTTP_TIMEOUT,
        max_retries: int = HTTP_MAX_RETRIES,
    ) -> None:
        self.timeout = timeout
        self.session = requests.Session()

        retry_strategy = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """
        Perform a GET request using the common HTTP configuration.
        """

        logger.debug("GET request: %s", url)

        try:
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )

        except requests.exceptions.Timeout as exc:
            raise APIConnectionError(
                f"Request timed out while connecting to {url}"
            ) from exc

        except requests.exceptions.ConnectionError as exc:
            raise APIConnectionError(
                f"Could not connect to {url}"
            ) from exc

        except requests.exceptions.RequestException as exc:
            raise APIConnectionError(
                f"Unexpected HTTP error while connecting to {url}: {exc}"
            ) from exc

        if response.status_code in (401, 403):
            raise APIAuthenticationError(
                f"Authentication failed for {url}. "
                f"HTTP status: {response.status_code}"
            )

        if not response.ok:
            raise APIRequestError(
                f"Request to {url} failed with "
                f"HTTP status {response.status_code}"
            )

        return response

    def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """
        Perform a POST request using the common HTTP configuration.
        """

        logger.debug("POST request: %s", url)

        try:
            response = self.session.post(
                url,
                data=data,
                headers=headers,
                timeout=self.timeout,
            )

        except requests.exceptions.Timeout as exc:
            raise APIConnectionError(
                f"Request timed out while connecting to {url}"
            ) from exc

        except requests.exceptions.ConnectionError as exc:
            raise APIConnectionError(
                f"Could not connect to {url}"
            ) from exc

        except requests.exceptions.RequestException as exc:
            raise APIConnectionError(
                f"Unexpected HTTP error while connecting to {url}: {exc}"
            ) from exc

        if response.status_code in (401, 403):
            raise APIAuthenticationError(
                f"Authentication failed for {url}. "
                f"HTTP status: {response.status_code}"
            )

        if not response.ok:
            raise APIRequestError(
                f"Request to {url} failed with "
                f"HTTP status {response.status_code}"
            )

        return response


    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | list[Any]:
        """
        Perform a GET request and deserialize the JSON response.
        """

        response = self.get(
            url,
            params=params,
            headers=headers,
        )

        if not response.content:
            raise EmptyResponseError(
                f"Empty response received from {url}"
            )

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as exc:
            raise APIResponseError(
                f"Invalid JSON response received from {url}"
            ) from exc

        if data is None or data == {} or data == []:
            raise EmptyResponseError(
                f"No data returned by {url}"
            )

        return data