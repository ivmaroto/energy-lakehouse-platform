"""
Ingestion logic for AEMET OpenData.
"""

from datetime import date
from pathlib import Path

from ingestion.aemet.client import AemetClient
from ingestion.common.logger import get_logger
from ingestion.common.storage import LocalBronzeStorage


logger = get_logger(__name__)


class AemetIngestion:
    """
    Coordinate AEMET extraction and Bronze persistence.
    """

    SOURCE = "aemet"
    DATASET = "daily_climatological_values"

    def __init__(
        self,
        client: AemetClient | None = None,
        storage: LocalBronzeStorage | None = None,
    ) -> None:
        self.client = client or AemetClient()
        self.storage = storage or LocalBronzeStorage()

    def ingest_historical(
        self,
        *,
        start_date: date,
        end_date: date,
        station_id: str,
    ) -> Path:
        """
        Retrieve historical daily climatological observations
        for an AEMET station and persist them in Bronze.
        """

        logger.info(
            "Starting AEMET historical ingestion "
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
            ingestion_mode="historical",
            requested_start_date=start_date.isoformat(),
            requested_end_date=end_date.isoformat(),
        )

        logger.info(
            "AEMET historical ingestion completed: %s",
            output_path,
        )

        return output_path

    def ingest_incremental(
        self,
        *,
        start_date: date,
        end_date: date,
        station_id: str,
    ) -> Path:
        """
        Retrieve a new AEMET temporal window and persist it in Bronze.

        Incremental ingestion uses the same AEMET dataset as the historical
        process. The difference is the requested temporal window.
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