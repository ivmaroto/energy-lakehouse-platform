"""
Ingestion logic for Open-Meteo data.
"""

from datetime import date, datetime
from pathlib import Path

from ingestion.common.config import OPEN_METEO_HISTORICAL_CHUNK_DAYS
from ingestion.common.date_utils import split_date_range
from ingestion.common.logger import get_logger
from ingestion.common.storage import MinIOBronzeStorage
from ingestion.open_meteo.client import OpenMeteoClient

from copy import deepcopy


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
    DATASET_MINUTELY_15 = "weather_15min"

    def __init__(
        self,
        client: OpenMeteoClient | None = None,
        storage: MinIOBronzeStorage | None = None,
    ) -> None:
        self.client = client or OpenMeteoClient()
        self.storage = storage or MinIOBronzeStorage()

    @staticmethod
    def _split_response_by_day(
            data: dict,
            *,
            section_name: str,
    ) -> dict[str, dict]:
        section = data.get(section_name)

        if not isinstance(section, dict):
            raise RuntimeError(
                f"Missing Open-Meteo section: {section_name}"
            )

        times = section.get("time")

        if not isinstance(times, list):
            raise RuntimeError(
                f"Invalid Open-Meteo time axis: {section_name}"
            )

        result: dict[str, dict] = {}

        for index, timestamp in enumerate(times):
            observation_date = str(timestamp)[:10]

            if observation_date not in result:
                daily_data = deepcopy(data)

                daily_section = {}

                for field, values in section.items():
                    if isinstance(values, list):
                        daily_section[field] = []

                    else:
                        daily_section[field] = deepcopy(values)

                daily_data[section_name] = daily_section
                result[observation_date] = daily_data

            daily_section = result[
                observation_date
            ][section_name]

            for field, values in section.items():
                if not isinstance(values, list):
                    continue

                if len(values) != len(times):
                    raise RuntimeError(
                        "Open-Meteo field length mismatch: "
                        f"{section_name}.{field}"
                    )

                daily_section[field].append(
                    values[index]
                )

        return result

    def ingest_historical(
            self,
            *,
            latitude: float,
            longitude: float,
            start_date: date,
            end_date: date,
            hourly_variables: list[str] | None = None,
            timezone: str = "UTC",
            station_id: str | None = None,
            station_name: str | None = None,
            province: str | None = None,
    ) -> list[Path | str]:

        if not station_id:
            raise ValueError(
                "station_id is required for canonical "
                "Open-Meteo Bronze persistence."
            )

        variables = (
                hourly_variables
                or DEFAULT_HOURLY_VARIABLES
        )

        chunks = split_date_range(
            start_date=start_date,
            end_date=end_date,
            chunk_days=OPEN_METEO_HISTORICAL_CHUNK_DAYS,
        )

        output_paths: list[Path | str] = []

        for chunk_start, chunk_end in chunks:
            data = (
                self.client
                .get_historical_weather(
                    latitude=latitude,
                    longitude=longitude,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    hourly_variables=variables,
                    timezone=timezone,
                )
            )

            daily_payloads = (
                self._split_response_by_day(
                    data,
                    section_name="hourly",
                )
            )

            for (
                    observation_date,
                    daily_data,
            ) in sorted(
                daily_payloads.items()
            ):
                year, month, day = (
                    observation_date.split("-")
                )

                object_name = (
                    "bronze/open_meteo/"
                    "weather_hourly/"
                    f"year={year}/"
                    f"month={month}/"
                    f"day={day}/"
                    f"station_id={station_id}.json"
                )

                output_path = (
                    self.storage.save_json(
                        daily_data,
                        source=self.SOURCE,
                        dataset=self.DATASET_HOURLY,
                        object_name=object_name,
                        ingestion_mode="historical",
                        requested_start_date=(
                            observation_date
                        ),
                        requested_end_date=(
                            observation_date
                        ),
                        extra_metadata={
                            "station_id": station_id,
                            "station_name": station_name,
                            "province": province,
                            "latitude": latitude,
                            "longitude": longitude,
                            "observation_date": (
                                observation_date
                            ),
                        },
                    )
                )

                output_paths.append(
                    output_path
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
            station_name: str | None = None,
            province: str | None = None,
    ) -> list[Path | str]:

        if not location_id:
            raise ValueError(
                "location_id is required for canonical "
                "Open-Meteo Bronze persistence."
            )

        variables = (
                minutely_15_variables
                or DEFAULT_MINUTELY_15_VARIABLES
        )

        data = (
            self.client
            .get_minutely_15_weather(
                latitude=latitude,
                longitude=longitude,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                minutely_15_variables=variables,
                timezone=timezone,
            )
        )

        daily_payloads = (
            self._split_response_by_day(
                data,
                section_name="minutely_15",
            )
        )

        output_paths: list[Path | str] = []

        for (
                observation_date,
                daily_data,
        ) in sorted(
            daily_payloads.items()
        ):
            year, month, day = (
                observation_date.split("-")
            )

            object_name = (
                "bronze/open_meteo/"
                "weather_15min/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/"
                f"station_id={location_id}.json"
            )

            output_path = (
                self.storage.save_json(
                    daily_data,
                    source=self.SOURCE,
                    dataset=self.DATASET_MINUTELY_15,
                    object_name=object_name,
                    ingestion_mode="incremental",
                    requested_start_date=(
                        start_datetime.isoformat()
                    ),
                    requested_end_date=(
                        end_datetime.isoformat()
                    ),
                    extra_metadata={
                        "location_id": location_id,
                        "station_id": location_id,
                        "station_name": station_name,
                        "province": province,
                        "latitude": latitude,
                        "longitude": longitude,
                        "observation_date": (
                            observation_date
                        ),
                    },
                )
            )

            output_paths.append(
                output_path
            )

        return output_paths
