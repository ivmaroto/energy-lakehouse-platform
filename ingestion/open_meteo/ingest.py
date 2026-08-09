"""
Ingestion logic for Open-Meteo data.
"""

from datetime import date
from pathlib import Path

from ingestion.common.logger import get_logger
from ingestion.common.storage import LocalBronzeStorage
from ingestion.open_meteo.client import OpenMeteoClient


logger = get_logger(__name__)


DEFAULT_HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "cloud_cover",
    "wind_speed_10m",
    "surface_pressure",
]


class OpenMeteoIngestion:
    """
    Coordinate Open-Meteo extraction and Bronze persistence.
    """

    SOURCE = "open_meteo"
    DATASET = "weather"

    def __init__(
        self,
        client: OpenMeteoClient | None = None,
        storage: LocalBronzeStorage | None = None,
    ) -> None:
        self.client = client or OpenMeteoClient()
        self.storage = storage or LocalBronzeStorage()

    def ingest_historical(
        self,
        *,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        hourly_variables: list[str] | None = None,
        timezone: str = "UTC",
    ) -> Path:
        """
        Retrieve historical Open-Meteo data and persist it in Bronze.
        """

        variables = hourly_variables or DEFAULT_HOURLY_VARIABLES

        logger.info(
            "Starting Open-Meteo historical ingestion: %s -> %s",
            start_date,
            end_date,
        )

        data = self.client.get_historical_weather(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            hourly_variables=variables,
            timezone=timezone,
        )

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=self.DATASET,
            ingestion_mode="historical",
            requested_start_date=start_date.isoformat(),
            requested_end_date=end_date.isoformat(),
        )

        logger.info(
            "Open-Meteo historical ingestion completed: %s",
            output_path,
        )

        return output_path

    def ingest_current(
        self,
        *,
        latitude: float,
        longitude: float,
        current_variables: list[str] | None = None,
        timezone: str = "UTC",
    ) -> Path:
        """
        Retrieve current Open-Meteo data and persist it in Bronze.
        """

        variables = current_variables or [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "cloud_cover",
            "wind_speed_10m",
            "surface_pressure",
        ]

        logger.info(
            "Starting Open-Meteo current ingestion."
        )

        data = self.client.get_current_weather(
            latitude=latitude,
            longitude=longitude,
            current_variables=variables,
            timezone=timezone,
        )

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=self.DATASET,
            ingestion_mode="incremental",
        )

        logger.info(
            "Open-Meteo current ingestion completed: %s",
            output_path,
        )

        return output_path