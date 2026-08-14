"""
Storage utilities for the Bronze ingestion layer.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from io import BytesIO

from minio import Minio
from minio.error import S3Error

from ingestion.common.config import (
    BRONZE_DIR,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)

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

class MinIOBronzeStorage:
    """
    Persist raw ingestion data in the MinIO Bronze layer.

    Objects are organized by source, dataset and ingestion date.
    """

    def __init__(
        self,
        endpoint: str = MINIO_ENDPOINT,
        access_key: str | None = MINIO_ACCESS_KEY,
        secret_key: str | None = MINIO_SECRET_KEY,
        bucket: str = MINIO_BUCKET,
        secure: bool = MINIO_SECURE,
    ) -> None:
        if not access_key or not secret_key:
            raise StorageError(
                "MinIO credentials are required."
            )

        self.bucket = bucket

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def save_json(
        self,
        data: dict[str, Any] | list[Any],
        *,
        source: str,
        dataset: str,
        ingestion_mode: str,
        requested_start_date: str | None = None,
        requested_end_date: str | None = None,
    ) -> str:
        """
        Persist a JSON response in the MinIO Bronze layer.

        Returns the generated object name.
        """

        ingestion_timestamp = datetime.now(timezone.utc)

        filename = (
            f"{source}_{dataset}_"
            f"{ingestion_timestamp:%Y%m%dT%H%M%S%fZ}.json"
        )

        object_name = (
            f"bronze/{source}/{dataset}/"
            f"year={ingestion_timestamp:%Y}/"
            f"month={ingestion_timestamp:%m}/"
            f"day={ingestion_timestamp:%d}/"
            f"{filename}"
        )

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

        try:
            json_bytes = json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")

            stream = BytesIO(json_bytes)

            if not self.client.bucket_exists(self.bucket):
                raise StorageError(
                    f"MinIO bucket '{self.bucket}' does not exist."
                )

            self.client.put_object(
                bucket_name=self.bucket,
                object_name=object_name,
                data=stream,
                length=len(json_bytes),
                content_type="application/json",
            )

        except S3Error as exc:
            raise StorageError(
                f"Could not persist Bronze data in MinIO for "
                f"{source}/{dataset}: {exc}"
            ) from exc

        logger.info(
            "Bronze data stored in MinIO: %s/%s",
            self.bucket,
            object_name,
        )

        return object_name