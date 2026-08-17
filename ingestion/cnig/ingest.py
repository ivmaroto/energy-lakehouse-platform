"""
Ingestion logic for CNIG / IGN data.
"""

from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

from ingestion.cnig.client import CnigClient
from ingestion.common.logger import get_logger
from ingestion.common.storage import (
    LocalBronzeStorage,
    MinIOBronzeStorage,
)


logger = get_logger(__name__)


class CnigIngestion:
    """
    Coordinate CNIG / IGN extraction and Bronze persistence.
    """

    SOURCE = "cnig"

    DATASET_PROVINCES = "provinces"
    DATASET_MUNICIPALITIES = "municipalities"

    PROVINCES_FILENAME = "PROVINCIAS.csv"
    MUNICIPALITIES_FILENAME = "MUNICIPIOS.csv"

    def __init__(
        self,
        client: CnigClient | None = None,
        storage: LocalBronzeStorage | MinIOBronzeStorage | None = None,
    ) -> None:
        self.client = client or CnigClient()
        self.storage = storage or MinIOBronzeStorage()

    def ingest_ngmep(self) -> list[Path | str]:
        """
        Download NGMEP and persist the required raw CSV files in Bronze.
        """

        logger.info(
            "Starting CNIG NGMEP ingestion."
        )

        zip_content = self.client.download_ngmep_zip()

        output_paths: list[Path | str] = []

        with ZipFile(BytesIO(zip_content)) as archive:
            names = archive.namelist()

            for filename, dataset in (
                (
                    self.PROVINCES_FILENAME,
                    self.DATASET_PROVINCES,
                ),
                (
                    self.MUNICIPALITIES_FILENAME,
                    self.DATASET_MUNICIPALITIES,
                ),
            ):
                if filename not in names:
                    raise ValueError(
                        f"Required CNIG file not found in NGMEP ZIP: "
                        f"{filename}"
                    )

                raw_bytes = archive.read(filename)

                output_path = self.storage.save_bytes(
                    raw_bytes,
                    source=self.SOURCE,
                    dataset=dataset,
                    ingestion_mode="snapshot",
                    extension="csv",
                    content_type="text/csv",
                )

                output_paths.append(output_path)

        logger.info(
            "CNIG NGMEP ingestion completed. "
            "%s Bronze files generated.",
            len(output_paths),
        )

        return output_paths