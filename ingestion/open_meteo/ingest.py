"""
Ingestion logic for Open-Meteo data.
"""

from datetime import date, datetime
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
    "dew_point_2m",
    "precipitation",
    "cloud_cover",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "sunshine_duration",
]

DEFAULT_HISTORICAL_FORECAST_VARIABLES = [
    "wind_speed_80m",
    "wind_direction_80m",
    "wind_speed_120m",
    "wind_direction_120m",
]

DEFAULT_MINUTELY_15_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "cloud_cover",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "wind_speed_80m",
    "wind_direction_80m",
    "wind_speed_120m",
    "wind_direction_120m",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "sunshine_duration",
]


class OpenMeteoIngestion:
    """
    Coordinate Open-Meteo extraction and Bronze persistence.
    """

    SOURCE = "open_meteo"

    DATASET_HOURLY = "weather_hourly"
    DATASET_HISTORICAL_FORECAST = "weather_historical_forecast"
    DATASET_MINUTELY_15 = "weather_15min"
    DATASET_CURRENT = "weather_current"

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
        Retrieve historical hourly Open-Meteo weather data in chunks.
        """

        variables = hourly_variables or DEFAULT_HOURLY_VARIABLES

        chunks = split_date_range(
            start_date=start_date,
            end_date=end_date,
            chunk_days=OPEN_METEO_HISTORICAL_CHUNK_DAYS,
        )

        logger.info(
            "Starting Open-Meteo historical hourly ingestion: %s -> %s "
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
                "Processing Open-Meteo hourly chunk %s/%s: %s -> %s",
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
                dataset=self.DATASET_HOURLY,
                ingestion_mode="historical",
                requested_start_date=chunk_start.isoformat(),
                requested_end_date=chunk_end.isoformat(),
            )

            output_paths.append(output_path)

        logger.info(
            "Open-Meteo historical hourly ingestion completed. "
            "%s Bronze files generated.",
            len(output_paths),
        )

        return output_paths

    def ingest_historical_forecast(
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
        Retrieve historical forecast data for validated wind variables
        at 80 m and 120 m.
        """

        variables = (
            hourly_variables
            or DEFAULT_HISTORICAL_FORECAST_VARIABLES
        )

        chunks = split_date_range(
            start_date=start_date,
            end_date=end_date,
            chunk_days=OPEN_METEO_HISTORICAL_CHUNK_DAYS,
        )

        logger.info(
            "Starting Open-Meteo historical forecast ingestion: %s -> %s "
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
                "Processing Open-Meteo historical forecast chunk "
                "%s/%s: %s -> %s",
                chunk_number,
                len(chunks),
                chunk_start,
                chunk_end,
            )

            data = self.client.get_historical_forecast(
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
                dataset=self.DATASET_HISTORICAL_FORECAST,
                ingestion_mode="historical",
                requested_start_date=chunk_start.isoformat(),
                requested_end_date=chunk_end.isoformat(),
            )

            output_paths.append(output_path)

        logger.info(
            "Open-Meteo historical forecast ingestion completed. "
            "%s Bronze files generated.",
            len(output_paths),
        )

        return output_paths


    def ingest_minutely_15(
            self,
            *,
            latitude: float,
            longitude: float,
            start_datetime: datetime,
            end_datetime: datetime,
            minutely_15_variables: list[str] | None = None,
            timezone: str = "UTC",
            location_id: str | None = None,
    ) -> Path | str:
        """
        Retrieve an exact 15-minute Open-Meteo temporal window
        and persist it in Bronze.
        """

        variables = (
                minutely_15_variables
                or DEFAULT_MINUTELY_15_VARIABLES
        )

        logger.info(
            "Starting Open-Meteo 15-minutely ingestion: %s -> %s",
            start_datetime,
            end_datetime,
        )

        data = self.client.get_minutely_15_weather(
            latitude=latitude,
            longitude=longitude,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            minutely_15_variables=variables,
            timezone=timezone,
        )

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=self.DATASET_MINUTELY_15,
            ingestion_mode="incremental",
            requested_start_date=start_datetime.isoformat(),
            requested_end_date=end_datetime.isoformat(),
            extra_metadata={
                "location_id": location_id,
                "latitude": latitude,
                "longitude": longitude,
            },
        )

        logger.info(
            "Open-Meteo 15-minutely ingestion completed: %s",
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
            dataset=self.DATASET_CURRENT,
            ingestion_mode="incremental",
        )

        logger.info(
            "Open-Meteo current ingestion completed: %s",
            output_path,
        )

        return output_path