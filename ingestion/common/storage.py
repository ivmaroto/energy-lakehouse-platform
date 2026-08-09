"""
Storage utilities for the Bronze ingestion layer.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.common.config import BRONZE_DIR
from ingestion.common.exceptions import StorageError
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


class LocalBronzeStorage:
    """
    Persist raw ingestion data in the local Bronze directory.

    Data is organized by source, dataset and ingestion date.
    """

    def __init__(self, base_path: Path = BRONZE_DIR) -> None:
        self.base_path = Path(base_path)

    def save_json(
        self,
        data: dict[str, Any] | list[Any],
        *,
        source: str,
        dataset: str,
        ingestion_mode: str,
        requested_start_date: str | None = None,
        requested_end_date: str | None = None,
    ) -> Path:
        """
        Persist a JSON response in the local Bronze layer.

        Returns the path of the generated file.
        """

        ingestion_timestamp = datetime.now(timezone.utc)

        target_directory = (
            self.base_path
            / source
            / dataset
            / f"year={ingestion_timestamp:%Y}"
            / f"month={ingestion_timestamp:%m}"
            / f"day={ingestion_timestamp:%d}"
        )

        try:
            target_directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            filename = (
                f"{source}_{dataset}_"
                f"{ingestion_timestamp:%Y%m%dT%H%M%S%fZ}.json"
            )

            output_path = target_directory / filename

            payload = {
                "metadata": {
                    "source": source,
                    "dataset": dataset,
                    "ingestion_mode": ingestion_mode,
                    "ingestion_timestamp": (
                        ingestion_timestamp.isoformat()
                    ),
                    "requested_start_date": requested_start_date,
                    "requested_end_date": requested_end_date,
                },
                "data": data,
            }

            with output_path.open(
                "w",
                encoding="utf-8",
            ) as file:
                json.dump(
                    payload,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

        except (OSError, TypeError, ValueError) as exc:
            raise StorageError(
                f"Could not persist Bronze data for "
                f"{source}/{dataset}: {exc}"
            ) from exc

        logger.info(
            "Bronze data stored: %s",
            output_path,
        )

        return output_path