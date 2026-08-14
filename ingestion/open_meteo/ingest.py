"""
Ingestion logic for Open-Meteo data.
"""

from datetime import date
from pathlib import Path

from ingestion.common.config import OPEN_METEO_HISTORICAL_CHUNK_DAYS
from ingestion.common.date_utils import split_date_range
from ingestion.common.logger import get_logger
from ingestion.common.storage import (
    LocalBronzeStorage,
    MinIOBronzeStorage,
)
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
            storage: LocalBronzeStorage | MinIOBronzeStorage | None = None,
    ) -> None:
        self.client = client or OpenMeteoClient()
        self.storage = storage or MinIOBronzeStorage()

    def ingest_historical(
        self,
        *,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        hourly_variables: list[str] | None = None,
        timezone: str = "UTC",
    ) -> list[Path | str]:
        """
        Retrieve historical Open-Meteo data in chunks and persist
        every successful chunk independently in Bronze.
        """

        variables = hourly_variables or DEFAULT_HOURLY_VARIABLES

        chunks = split_date_range(
            start_date=start_date,
            end_date=end_date,
            chunk_days=OPEN_METEO_HISTORICAL_CHUNK_DAYS,
        )

        logger.info(
            "Starting Open-Meteo historical ingestion: %s -> %s "
            "(%s chunks)",
            start_date,
            end_date,
            len(chunks),
        )

        output_paths: list[Path | str] = []

        for chunk_number, (chunk_start, chunk_end) in enumerate(
            chunks,
            start=1,
        ):
            logger.info(
                "Processing Open-Meteo chunk %s/%s: %s -> %s",
                chunk_number,
                len(chunks),
                chunk_start,
                chunk_end,
            )

            data = self.client.get_historical_weather(
                latitude=latitude,
                longitude=longitude,
                start_date=chunk_start,
                end_date=chunk_end,
                hourly_variables=variables,
                timezone=timezone,
            )

            output_path = self.storage.save_json(
                data,
                source=self.SOURCE,
                dataset=self.DATASET,
                ingestion_mode="historical",
                requested_start_date=chunk_start.isoformat(),
                requested_end_date=chunk_end.isoformat(),
            )

            output_paths.append(output_path)

        logger.info(
            "Open-Meteo historical ingestion completed. "
            "%s Bronze files generated.",
            len(output_paths),
        )

        return output_paths

    def ingest_current(
        self,
        *,
        latitude: float,
        longitude: float,
        current_variables: list[str] | None = None,
        timezone: str = "UTC",
    ) -> Path | str:
        """
        Retrieve current Open-Meteo data and persist it in Bronze.
        """

        variables = current_variables or DEFAULT_HOURLY_VARIABLES

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