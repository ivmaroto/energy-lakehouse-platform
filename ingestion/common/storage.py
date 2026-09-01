"""
Storage utilities for the Bronze ingestion layer.
"""

import json

from datetime import datetime, timezone
from io import BytesIO
from typing import Any

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
    Persist Bronze objects in MinIO.

    Object paths are supplied explicitly by the ingestion layer.

    The storage layer does not decide whether an object represents:
        - a master dataset;
        - a daily observation partition;
        - a monthly observation partition.

    ingestion_timestamp remains technical audit metadata and does not
    determine the physical Bronze location.
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
    def _validate_object_name(
        object_name: str,
    ) -> str:
        value = object_name.strip()

        if not value:
            raise ValueError(
                "object_name cannot be empty."
            )

        if not value.startswith(
            "bronze/"
        ):
            raise ValueError(
                "Bronze object_name must start with 'bronze/'."
            )

        return value

    def _put_object(
        self,
        *,
        object_name: str,
        data: bytes,
        content_type: str,
        source: str,
        dataset: str,
    ) -> None:
        object_name = (
            self._validate_object_name(
                object_name
            )
        )

        try:
            if not self.client.bucket_exists(
                self.bucket
            ):
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
        object_name: str,
        content_type: str,
    ) -> str:
        """
        Persist a raw binary response at an explicit Bronze object path.
        """

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
        object_name: str,
        ingestion_mode: str,
        requested_start_date: str | None = None,
        requested_end_date: str | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Persist a JSON Bronze wrapper at an explicit object path.

        ingestion_timestamp is technical audit metadata only.
        """

        ingestion_timestamp = datetime.now(
            timezone.utc
        )

        metadata = {
            "source": source,
            "dataset": dataset,
            "ingestion_mode": ingestion_mode,
            "ingestion_timestamp": (
                ingestion_timestamp.isoformat()
            ),
            "requested_start_date": (
                requested_start_date
            ),
            "requested_end_date": (
                requested_end_date
            ),
        }

        if extra_metadata:
            metadata.update(
                extra_metadata
            )

        payload = {
            "metadata": metadata,
            "data": data,
        }

        try:
            json_bytes = json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            ).encode(
                "utf-8"
            )

        except (
            TypeError,
            ValueError,
        ) as exc:
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


    def object_exists(
        self,
        object_name: str,
    ) -> bool:
        """
        Return True only when the exact Bronze object exists.
        """

        object_name = (
            self._validate_object_name(
                object_name
            )
        )

        try:
            objects = (
                self.client.list_objects(
                    self.bucket,
                    prefix=object_name,
                    recursive=True,
                )
            )

            return any(
                obj.object_name
                == object_name
                for obj in objects
            )

        except S3Error as exc:
            raise StorageError(
                "Could not inspect Bronze object in MinIO: "
                f"{object_name}: {exc}"
            ) from exc

    def read_json(
        self,
        object_name: str,
    ) -> dict[str, Any] | list[Any]:
        """
        Read and deserialize one exact Bronze JSON object.
        """

        object_name = (
            self._validate_object_name(
                object_name
            )
        )

        response = None

        try:
            response = (
                self.client.get_object(
                    self.bucket,
                    object_name,
                )
            )

            raw_data = (
                response
                .read()
                .decode("utf-8")
            )

            return json.loads(
                raw_data
            )

        except S3Error as exc:
            raise StorageError(
                "Could not read Bronze JSON object from MinIO: "
                f"{object_name}: {exc}"
            ) from exc

        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as exc:
            raise StorageError(
                "Invalid Bronze JSON object: "
                f"{object_name}: {exc}"
            ) from exc

        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def delete_prefix(
            self,
            prefix: str,
    ) -> int:
        """
        Delete active Bronze objects below one prefix.

        Backup objects are never removed.
        """

        prefix = prefix.strip()

        if not prefix:
            raise ValueError(
                "prefix cannot be empty."
            )

        if not prefix.startswith(
                "bronze/"
        ):
            raise ValueError(
                "Bronze deletion prefix must start with 'bronze/'."
            )

        if "backup_before_reload_" in prefix:
            raise ValueError(
                "Backup prefixes cannot be deleted."
            )

        deleted = 0

        try:
            objects = list(
                self.client.list_objects(
                    self.bucket,
                    prefix=prefix,
                    recursive=True,
                )
            )

            for obj in objects:
                object_name = (
                    obj.object_name
                )

                # Defensive protection.
                if (
                        "backup_before_reload_"
                        in object_name
                ):
                    logger.warning(
                        "Protected Bronze backup object skipped: %s",
                        object_name,
                    )
                    continue

                self.client.remove_object(
                    self.bucket,
                    object_name,
                )

                deleted += 1

        except S3Error as exc:
            raise StorageError(
                "Could not delete Bronze prefix "
                f"{prefix}: {exc}"
            ) from exc

        logger.info(
            "Bronze prefix deleted: "
            "prefix=%s objects=%s",
            prefix,
            deleted,
        )

        return deleted

    def delete_warehouse_layer(
            self,
            prefix: str,
    ) -> int:
        """
        Delete all physical objects from an approved Iceberg
        warehouse layer during a complete historical reset.
        """

        allowed_prefixes = {
            "warehouse/silver/",
            "warehouse/gold/",
        }

        prefix = prefix.strip()

        if prefix not in allowed_prefixes:
            raise ValueError(
                "Warehouse deletion is restricted to "
                "warehouse/silver/ and warehouse/gold/."
            )

        objects = list(
            self.client.list_objects(
                self.bucket,
                prefix=prefix,
                recursive=True,
            )
        )

        deleted = 0

        for obj in objects:
            self.client.remove_object(
                self.bucket,
                obj.object_name,
            )
            deleted += 1

        print(
            f"WAREHOUSE_PREFIX = {prefix}"
        )
        print(
            f"DELETED_OBJECTS = {deleted}"
        )

        return deleted