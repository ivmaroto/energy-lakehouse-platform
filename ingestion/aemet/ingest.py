"""
Ingestion logic for AEMET OpenData.
"""

from datetime import date
from pathlib import Path

from ingestion.aemet.client import AemetClient
from ingestion.common.config import AEMET_HISTORICAL_CHUNK_DAYS
from ingestion.common.date_utils import split_date_range
from ingestion.common.logger import get_logger
from ingestion.common.storage import (
    LocalBronzeStorage,
    MinIOBronzeStorage,
)


logger = get_logger(__name__)


class AemetIngestion:
    """
    Coordinate AEMET extraction and Bronze persistence.
    """

    SOURCE = "aemet"

    DATASET = "daily_climatological_values"
    DATASET_STATIONS = "stations"
    DATASET_CURRENT_OBSERVATIONS = "current_observations"
    DATASET_RADIATION = "radiation"

    def __init__(
        self,
        client: AemetClient | None = None,
        storage: LocalBronzeStorage | MinIOBronzeStorage | None = None,
    ) -> None:
        self.client = client or AemetClient()
        self.storage = storage or MinIOBronzeStorage()

    def ingest_stations(
        self,
    ) -> Path | str:
        """
        Retrieve the AEMET climatological station inventory
        and persist it in Bronze.
        """

        logger.info(
            "Starting AEMET station inventory ingestion."
        )

        data = self.client.get_stations()

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=self.DATASET_STATIONS,
            ingestion_mode="snapshot",
        )

        logger.info(
            "AEMET station inventory ingestion completed: %s",
            output_path,
        )

        return output_path

    def ingest_current_observations(
        self,
    ) -> Path | str:
        """
        Retrieve current conventional observations
        from all available AEMET stations
        and persist the raw response in Bronze.
        """

        logger.info(
            "Starting AEMET conventional observations ingestion."
        )

        data = self.client.get_current_observations()

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=self.DATASET_CURRENT_OBSERVATIONS,
            ingestion_mode="incremental",
        )

        logger.info(
            "AEMET conventional observations ingestion completed: %s",
            output_path,
        )

        return output_path

    def ingest_radiation(
        self,
    ) -> Path | str:
        """
        Retrieve the AEMET special radiation network dataset
        and persist the raw CSV response in Bronze.

        Parsing and normalization are intentionally deferred
        to downstream processing layers.
        """

        logger.info(
            "Starting AEMET radiation ingestion."
        )

        raw_data = self.client.get_radiation_data()

        output_path = self.storage.save_text(
            raw_data,
            source=self.SOURCE,
            dataset=self.DATASET_RADIATION,
            ingestion_mode="incremental",
            extension="csv",
            content_type="text/csv",
        )

        logger.info(
            "AEMET radiation ingestion completed: %s",
            output_path,
        )

        return output_path

    def ingest_historical(
        self,
        *,
        start_date: date,
        end_date: date,
        station_id: str,
    ) -> list[Path | str]:
        """
        Retrieve historical daily climatological observations
        in chunks and persist each chunk independently in Bronze.
        """

        chunks = split_date_range(
            start_date=start_date,
            end_date=end_date,
            chunk_days=AEMET_HISTORICAL_CHUNK_DAYS,
        )

        logger.info(
            "Starting AEMET historical ingestion "
            "for station=%s, period=%s -> %s (%s chunks)",
            station_id,
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
                "Processing AEMET chunk %s/%s: %s -> %s",
                chunk_number,
                len(chunks),
                chunk_start,
                chunk_end,
            )

            data = self.client.get_daily_climatological_values(
                start_date=chunk_start,
                end_date=chunk_end,
                station_id=station_id,
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
            "AEMET historical ingestion completed. "
            "%s Bronze files generated.",
            len(output_paths),
        )

        return output_paths

    def ingest_incremental(
        self,
        *,
        start_date: date,
        end_date: date,
        station_id: str,
    ) -> Path | str:
        """
        Retrieve a new AEMET climatological temporal window
        and persist it in Bronze.
        """

        logger.info(
            "Starting AEMET incremental ingestion "
            "for station=%s, period=%s -> %s",
            station_id,
            start_date,
            end_date,
        )

        data = self.client.get_daily_climatological_values(
            start_date=start_date,
            end_date=end_date,
            station_id=station_id,
        )

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=self.DATASET,
            ingestion_mode="incremental",
            requested_start_date=start_date.isoformat(),
            requested_end_date=end_date.isoformat(),
        )

        logger.info(
            "AEMET incremental ingestion completed: %s",
            output_path,
        )

        return output_path