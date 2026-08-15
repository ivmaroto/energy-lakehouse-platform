"""
Client for the REE / ESIOS API.
"""

from datetime import date, datetime, timezone
from typing import Any

from ingestion.common.config import ESIOS_API_KEY, ESIOS_BASE_URL
from ingestion.common.exceptions import (
    ConfigurationError,
    InvalidDateRangeError,
)
from ingestion.common.http_client import HTTPClient
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


class EsiosClient:
    """
    Client used to retrieve energy data from the REE / ESIOS API.
    """

    def __init__(
        self,
        api_key: str | None = ESIOS_API_KEY,
        http_client: HTTPClient | None = None,
    ) -> None:
        if not api_key:
            raise ConfigurationError(
                "ESIOS_API_KEY is required to use the ESIOS connector."
            )

        self.api_key = api_key
        self.http_client = http_client or HTTPClient()

    @property
    def headers(self) -> dict[str, str]:
        """Return the HTTP headers required by ESIOS."""

        return {
            "Accept": "application/json; application/vnd.esios-api-v1+json",
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
        }

    @staticmethod
    def _validate_date_range(
        start_date: date | datetime,
        end_date: date | datetime,
    ) -> None:
        """Validate the requested temporal interval."""

        if start_date > end_date:
            raise InvalidDateRangeError(
                f"Invalid date range: {start_date} is after {end_date}."
            )

    @staticmethod
    def _format_start_date(
        value: date | datetime,
    ) -> str:
        """
        Format the beginning of a temporal interval for ESIOS.

        Date values are expanded to the beginning of the day.
        Datetime values preserve their exact time and are normalized to UTC.
        """

        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)

            return value.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        return f"{value.isoformat()}T00:00:00Z"

    @staticmethod
    def _format_end_date(
        value: date | datetime,
    ) -> str:
        """
        Format the end of a temporal interval for ESIOS.

        Date values are expanded to the end of the day.
        Datetime values preserve their exact time and are normalized to UTC.
        """

        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)

            return value.astimezone(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        return f"{value.isoformat()}T23:59:59Z"

    def get_indicators(
        self,
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve the list of available ESIOS indicators.
        """

        endpoint = f"{ESIOS_BASE_URL}/indicators"

        logger.info(
            "Requesting ESIOS indicator catalogue."
        )

        return self.http_client.get_json(
            endpoint,
            headers=self.headers,
        )

    def get_indicator(
        self,
        *,
        indicator_id: int,
        start_date: date | datetime,
        end_date: date | datetime,
        time_trunc: str | None = None,
        time_agg: str | None = None,
        geo_ids: list[int] | None = None,
        geo_trunc: str | None = None,
        geo_agg: str | None = None,
    ) -> dict[str, Any] | list[Any]:
        """
        Retrieve values for a specific ESIOS indicator.
        """

        self._validate_date_range(
            start_date,
            end_date,
        )

        if indicator_id <= 0:
            raise ValueError(
                "A valid positive ESIOS indicator ID must be provided."
            )

        endpoint = (
            f"{ESIOS_BASE_URL}/indicators/{indicator_id}"
        )

        params: dict[str, Any] = {
            "start_date": self._format_start_date(
                start_date
            ),
            "end_date": self._format_end_date(
                end_date
            ),
        }

        if time_trunc:
            params["time_trunc"] = time_trunc

        if time_agg:
            params["time_agg"] = time_agg

        if geo_ids:
            params["geo_ids[]"] = geo_ids

        if geo_trunc:
            params["geo_trunc"] = geo_trunc

        if geo_agg:
            params["geo_agg"] = geo_agg

        logger.info(
            "Requesting ESIOS indicator=%s, period=%s -> %s",
            indicator_id,
            start_date,
            end_date,
        )

        return self.http_client.get_json(
            endpoint,
            params=params,
            headers=self.headers,
        )