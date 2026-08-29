"""
Storage utilities for the Bronze ingestion layer.
"""

import json

from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from uuid import uuid4

from minio import Minio
from minio.error import S3Error

from ingestion.common.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)
from ingestion.common.exceptions import StorageError
from ingestion.common.logger import get_logger


logger = get_logger(__name__)


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

    @staticmethod
    def _build_object_name(
        *,
        source: str,
        dataset: str,
        extension: str,
        ingestion_timestamp: datetime,
    ) -> str:
        filename = (
            f"{source}_{dataset}_"
            f"{ingestion_timestamp:%Y%m%dT%H%M%S%fZ}_"
            f"{uuid4().hex}.{extension}"
        )

        return (
            f"bronze/{source}/{dataset}/"
            f"year={ingestion_timestamp:%Y}/"
            f"month={ingestion_timestamp:%m}/"
            f"day={ingestion_timestamp:%d}/"
            f"{filename}"
        )

    def _put_object(
        self,
        *,
        object_name: str,
        data: bytes,
        content_type: str,
        source: str,
        dataset: str,
    ) -> None:
        try:
            if not self.client.bucket_exists(self.bucket):
                raise StorageError(
                    f"MinIO bucket '{self.bucket}' does not exist."
                )

            self.client.put_object(
                bucket_name=self.bucket,
                object_name=object_name,
                data=BytesIO(data),
                length=len(data),
                content_type=content_type,
            )

        except S3Error as exc:
            raise StorageError(
                f"Could not persist Bronze data in MinIO for "
                f"{source}/{dataset}: {exc}"
            ) from exc

    def save_bytes(
        self,
        data: bytes,
        *,
        source: str,
        dataset: str,
        extension: str,
        content_type: str,
    ) -> str:
        """
        Persist a raw binary response in Bronze.
        """

        ingestion_timestamp = datetime.now(timezone.utc)

        object_name = self._build_object_name(
            source=source,
            dataset=dataset,
            extension=extension,
            ingestion_timestamp=ingestion_timestamp,
        )

        self._put_object(
            object_name=object_name,
            data=data,
            content_type=content_type,
            source=source,
            dataset=dataset,
        )

        logger.info(
            "Bronze binary data stored in MinIO: %s/%s",
            self.bucket,
            object_name,
        )

        return object_name

    def save_json(
        self,
        data: dict[str, Any] | list[Any],
        *,
        source: str,
        dataset: str,
        ingestion_mode: str,
        requested_start_date: str | None = None,
        requested_end_date: str | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Persist a JSON response in Bronze with ingestion metadata.
        """

        ingestion_timestamp = datetime.now(timezone.utc)

        object_name = self._build_object_name(
            source=source,
            dataset=dataset,
            extension="json",
            ingestion_timestamp=ingestion_timestamp,
        )

        metadata = {
            "source": source,
            "dataset": dataset,
            "ingestion_mode": ingestion_mode,
            "ingestion_timestamp": ingestion_timestamp.isoformat(),
            "requested_start_date": requested_start_date,
            "requested_end_date": requested_end_date,
        }

        if extra_metadata:
            metadata.update(extra_metadata)

        payload = {
            "metadata": metadata,
            "data": data,
        }

        try:
            json_bytes = json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise StorageError(
                f"Could not serialize Bronze data for "
                f"{source}/{dataset}: {exc}"
            ) from exc

        self._put_object(
            object_name=object_name,
            data=json_bytes,
            content_type="application/json",
            source=source,
            dataset=dataset,
        )

        logger.info(
            "Bronze data stored in MinIO: %s/%s",
            self.bucket,
            object_name,
        )

        return object_name
