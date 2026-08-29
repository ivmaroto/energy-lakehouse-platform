"""
Ingestion logic for AEMET OpenData.
"""

from pathlib import Path

from ingestion.aemet.client import AemetClient
from ingestion.common.logger import get_logger
from ingestion.common.storage import MinIOBronzeStorage


logger = get_logger(__name__)


class AemetIngestion:
    """
    Coordinate AEMET extraction and Bronze persistence.
    """

    SOURCE = "aemet"

    DATASET_STATIONS = "stations"
    DATASET_CURRENT_OBSERVATIONS = "current_observations"

    def __init__(
        self,
        client: AemetClient | None = None,
        storage: MinIOBronzeStorage | None = None,
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
