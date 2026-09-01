"""
Ingestion logic for REE / ESIOS data.
"""

from __future__ import annotations

import calendar

from copy import deepcopy
from datetime import (
    date,
    datetime,
    timezone,
)
from pathlib import Path

from ingestion.common.config import (
    ESIOS_HISTORICAL_CHUNK_DAYS,
)
from ingestion.common.date_utils import (
    split_date_range,
)
from ingestion.common.logger import (
    get_logger,
)
from ingestion.common.storage import (
    MinIOBronzeStorage,
)
from ingestion.esios.client import (
    EsiosClient,
)


logger = get_logger(__name__)


TIME_HOUR_ID = 4
TIME_MONTH_ID = 2


class EsiosIngestion:
    """
    Coordinate REE / ESIOS extraction and canonical Bronze persistence.

    Bronze temporal organization:

        Hourly observations
            -> UTC observation day

        Monthly observations
            -> UTC observation month

    Bronze observation key:

        geo_id + datetime_utc

    A canonical partition object is merged and rewritten rather than
    duplicated when the same observation is received again.
    """

    SOURCE = "esios"

    def __init__(
        self,
        client: EsiosClient | None = None,
        storage: MinIOBronzeStorage | None = None,
    ) -> None:
        self.client = (
            client
            or EsiosClient()
        )

        self.storage = (
            storage
            or MinIOBronzeStorage()
        )

    # ========================================================================
    # Response validation
    # ========================================================================

    @staticmethod
    def _get_indicator(
        data,
        *,
        indicator_id: int,
        dataset: str,
    ) -> dict:
        if not isinstance(
            data,
            dict,
        ):
            raise RuntimeError(
                "Invalid ESIOS response for "
                f"indicator={indicator_id}, "
                f"dataset={dataset}: "
                "expected JSON object."
            )

        indicator = data.get(
            "indicator"
        )

        if not isinstance(
            indicator,
            dict,
        ):
            raise RuntimeError(
                "Invalid ESIOS response for "
                f"indicator={indicator_id}, "
                f"dataset={dataset}: "
                "missing indicator object."
            )

        values = indicator.get(
            "values"
        )

        if not isinstance(
            values,
            list,
        ):
            raise RuntimeError(
                "Invalid ESIOS response for "
                f"indicator={indicator_id}, "
                f"dataset={dataset}: "
                "missing values array."
            )

        return indicator

    @staticmethod
    def _get_time_id(
        indicator: dict,
        *,
        indicator_id: int,
        dataset: str,
    ) -> int:
        definitions = indicator.get(
            "tiempo"
        )

        if not isinstance(
            definitions,
            list,
        ) or not definitions:
            raise RuntimeError(
                "Invalid ESIOS temporal definition for "
                f"indicator={indicator_id}, "
                f"dataset={dataset}."
            )

        definition = definitions[0]

        if not isinstance(
            definition,
            dict,
        ):
            raise RuntimeError(
                "Invalid ESIOS temporal definition for "
                f"indicator={indicator_id}, "
                f"dataset={dataset}."
            )

        time_id = definition.get(
            "id"
        )

        try:
            return int(
                time_id
            )

        except (
            TypeError,
            ValueError,
        ) as exc:
            raise RuntimeError(
                "Invalid ESIOS time id for "
                f"indicator={indicator_id}, "
                f"dataset={dataset}: "
                f"{time_id}"
            ) from exc

    # ========================================================================
    # Observation timestamps
    # ========================================================================

    @staticmethod
    def _parse_datetime_utc(
        value,
    ) -> datetime:
        if not value:
            raise RuntimeError(
                "ESIOS observation without datetime_utc."
            )

        text = str(
            value
        )

        if text.endswith(
            "Z"
        ):
            text = (
                text[:-1]
                + "+00:00"
            )

        try:
            parsed = (
                datetime.fromisoformat(
                    text
                )
            )

        except ValueError as exc:
            raise RuntimeError(
                "Invalid ESIOS datetime_utc: "
                f"{value}"
            ) from exc

        if parsed.tzinfo is None:
            raise RuntimeError(
                "ESIOS datetime_utc without timezone: "
                f"{value}"
            )

        return parsed.astimezone(
            timezone.utc
        )

    # ========================================================================
    # Bronze natural key
    # ========================================================================

    @classmethod
    def _value_key(
        cls,
        value: dict,
    ) -> tuple[str, str]:
        if not isinstance(
            value,
            dict,
        ):
            raise RuntimeError(
                "Invalid ESIOS observation: "
                "expected JSON object."
            )

        geo_id = value.get(
            "geo_id"
        )

        datetime_utc = value.get(
            "datetime_utc"
        )

        if geo_id is None:
            raise RuntimeError(
                "ESIOS observation without geo_id."
            )

        cls._parse_datetime_utc(
            datetime_utc
        )

        return (
            str(
                geo_id
            ),
            str(
                datetime_utc
            ),
        )

    # ========================================================================
    # Partitioning
    # ========================================================================

    @classmethod
    def _partition_values(
        cls,
        values: list[dict],
        *,
        time_id: int,
    ) -> dict[str, list[dict]]:
        partitions: dict[
            str,
            list[dict],
        ] = {}

        for value in values:
            observation_datetime = (
                cls._parse_datetime_utc(
                    value.get(
                        "datetime_utc"
                    )
                    if isinstance(
                        value,
                        dict,
                    )
                    else None
                )
            )

            if time_id == TIME_HOUR_ID:
                partition = (
                    observation_datetime
                    .date()
                    .isoformat()
                )

            elif time_id == TIME_MONTH_ID:
                partition = (
                    observation_datetime
                    .strftime(
                        "%Y-%m"
                    )
                )

            else:
                raise RuntimeError(
                    "Unsupported ESIOS temporal grain: "
                    f"time_id={time_id}"
                )

            partitions.setdefault(
                partition,
                [],
            ).append(
                value
            )

        return partitions

    @staticmethod
    def _object_name(
        *,
        dataset: str,
        partition: str,
        time_id: int,
    ) -> str:
        if time_id == TIME_HOUR_ID:
            year, month, day = (
                partition.split(
                    "-"
                )
            )

            return (
                f"bronze/esios/{dataset}/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/"
                "data.json"
            )

        if time_id == TIME_MONTH_ID:
            year, month = (
                partition.split(
                    "-"
                )
            )

            return (
                f"bronze/esios/{dataset}/"
                f"year={year}/"
                f"month={month}/"
                "data.json"
            )

        raise RuntimeError(
            "Unsupported ESIOS temporal grain: "
            f"time_id={time_id}"
        )

    # ========================================================================
    # Existing canonical object
    # ========================================================================

    def _existing_values(
        self,
        *,
        object_name: str,
        indicator_id: int,
    ) -> list[dict]:
        if not self.storage.object_exists(
            object_name
        ):
            return []

        payload = (
            self.storage.read_json(
                object_name
            )
        )

        if not isinstance(
            payload,
            dict,
        ):
            raise RuntimeError(
                "Invalid existing ESIOS Bronze wrapper: "
                f"{object_name}"
            )

        data = payload.get(
            "data"
        )

        if not isinstance(
            data,
            dict,
        ):
            raise RuntimeError(
                "Invalid existing ESIOS Bronze data: "
                f"{object_name}"
            )

        indicator = data.get(
            "indicator"
        )

        if not isinstance(
            indicator,
            dict,
        ):
            raise RuntimeError(
                "Invalid existing ESIOS indicator: "
                f"{object_name}"
            )

        existing_indicator_id = (
            indicator.get(
                "id"
            )
        )

        if str(
            existing_indicator_id
        ) != str(
            indicator_id
        ):
            raise RuntimeError(
                "Existing ESIOS Bronze indicator mismatch: "
                f"{object_name}"
            )

        values = indicator.get(
            "values"
        )

        if not isinstance(
            values,
            list,
        ):
            raise RuntimeError(
                "Invalid existing ESIOS values: "
                f"{object_name}"
            )

        return values

    @classmethod
    def _merge_values(
        cls,
        existing_values: list[dict],
        new_values: list[dict],
    ) -> list[dict]:
        merged: dict[
            tuple[str, str],
            dict,
        ] = {}

        for value in (
            existing_values
        ):
            merged[
                cls._value_key(
                    value
                )
            ] = value

        # New API observations deliberately prevail.
        for value in (
            new_values
        ):
            merged[
                cls._value_key(
                    value
                )
            ] = value

        return [
            merged[key]
            for key in sorted(
                merged,
                key=lambda item: (
                    item[1],
                    item[0],
                ),
            )
        ]

    # ========================================================================
    # Canonical persistence
    # ========================================================================

    def _persist_response(
        self,
        data: dict,
        *,
        indicator_id: int,
        dataset: str,
        ingestion_mode: str,
        requested_start_date: str,
        requested_end_date: str,
    ) -> list[Path | str]:
        indicator = (
            self._get_indicator(
                data,
                indicator_id=indicator_id,
                dataset=dataset,
            )
        )

        values = indicator[
            "values"
        ]

        if not values:
            logger.info(
                "ESIOS NO_DATA: "
                "indicator=%s dataset=%s "
                "requested=%s -> %s",
                indicator_id,
                dataset,
                requested_start_date,
                requested_end_date,
            )

            return []

        time_id = (
            self._get_time_id(
                indicator,
                indicator_id=indicator_id,
                dataset=dataset,
            )
        )

        if time_id not in {
            TIME_HOUR_ID,
            TIME_MONTH_ID,
        }:
            raise RuntimeError(
                "Unsupported ESIOS temporal grain "
                f"for indicator={indicator_id}: "
                f"time_id={time_id}"
            )

        partitions = (
            self._partition_values(
                values,
                time_id=time_id,
            )
        )

        output_paths: list[
            Path | str
        ] = []

        for partition in sorted(
            partitions
        ):
            object_name = (
                self._object_name(
                    dataset=dataset,
                    partition=partition,
                    time_id=time_id,
                )
            )

            existing_values = (
                self._existing_values(
                    object_name=object_name,
                    indicator_id=indicator_id,
                )
            )

            merged_values = (
                self._merge_values(
                    existing_values,
                    partitions[
                        partition
                    ],
                )
            )

            partition_data = (
                deepcopy(
                    data
                )
            )

            partition_data[
                "indicator"
            ][
                "values"
            ] = merged_values

            extra_metadata = {
                "time_id": time_id,
            }

            if time_id == TIME_HOUR_ID:
                extra_metadata[
                    "observation_date"
                ] = partition

            else:
                extra_metadata[
                    "observation_month"
                ] = partition

            output_path = (
                self.storage.save_json(
                    partition_data,
                    source=self.SOURCE,
                    dataset=dataset,
                    object_name=object_name,
                    ingestion_mode=(
                        ingestion_mode
                    ),
                    requested_start_date=(
                        requested_start_date
                    ),
                    requested_end_date=(
                        requested_end_date
                    ),
                    extra_metadata=(
                        extra_metadata
                    ),
                )
            )

            output_paths.append(
                output_path
            )

            logger.info(
                "ESIOS Bronze partition persisted: "
                "indicator=%s dataset=%s "
                "partition=%s rows=%s object=%s",
                indicator_id,
                dataset,
                partition,
                len(
                    merged_values
                ),
                object_name,
            )

        return output_paths

    # ========================================================================
    # Historical
    # ========================================================================

    def ingest_historical(
        self,
        *,
        indicator_id: int,
        dataset: str,
        start_date: date,
        end_date: date,
        time_trunc: str | None = None,
        time_agg: str | None = None,
        geo_ids: list[int] | None = None,
        geo_trunc: str | None = None,
        geo_agg: str | None = None,
    ) -> list[Path | str]:
        """
        Retrieve historical ESIOS values and persist canonical UTC
        observation partitions in Bronze.
        """

        chunks = split_date_range(
            start_date=start_date,
            end_date=end_date,
            chunk_days=(
                ESIOS_HISTORICAL_CHUNK_DAYS
            ),
        )

        logger.info(
            "Starting ESIOS historical ingestion "
            "for indicator=%s, period=%s -> %s "
            "(%s chunks)",
            indicator_id,
            start_date,
            end_date,
            len(
                chunks
            ),
        )

        output_paths: list[
            Path | str
        ] = []

        for (
            chunk_start,
            chunk_end,
        ) in chunks:
            data = (
                self.client.get_indicator(
                    indicator_id=indicator_id,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    time_trunc=time_trunc,
                    time_agg=time_agg,
                    geo_ids=geo_ids,
                    geo_trunc=geo_trunc,
                    geo_agg=geo_agg,
                )
            )

            paths = (
                self._persist_response(
                    data,
                    indicator_id=indicator_id,
                    dataset=dataset,
                    ingestion_mode=(
                        "historical"
                    ),
                    requested_start_date=(
                        chunk_start.isoformat()
                    ),
                    requested_end_date=(
                        chunk_end.isoformat()
                    ),
                )
            )

            output_paths.extend(
                paths
            )

        logger.info(
            "ESIOS historical ingestion completed. "
            "%s canonical Bronze objects written.",
            len(
                output_paths
            ),
        )

        return output_paths

    # ========================================================================
    # Incremental
    # ========================================================================

    def ingest_incremental(
        self,
        *,
        indicator_id: int,
        dataset: str,
        start_date: date | datetime,
        end_date: date | datetime,
        time_trunc: str | None = None,
        time_agg: str | None = None,
        geo_ids: list[int] | None = None,
        geo_trunc: str | None = None,
        geo_agg: str | None = None,
    ) -> list[Path | str]:
        """
        Retrieve one ESIOS temporal window and persist canonical UTC
        observation partitions in Bronze.

        A valid ESIOS response with values=[] is NO_DATA and returns [].
        """

        logger.info(
            "Starting ESIOS incremental ingestion "
            "for indicator=%s, period=%s -> %s",
            indicator_id,
            start_date,
            end_date,
        )

        data = (
            self.client.get_indicator(
                indicator_id=indicator_id,
                start_date=start_date,
                end_date=end_date,
                time_trunc=time_trunc,
                time_agg=time_agg,
                geo_ids=geo_ids,
                geo_trunc=geo_trunc,
                geo_agg=geo_agg,
            )
        )

        return self._persist_response(
            data,
            indicator_id=indicator_id,
            dataset=dataset,
            ingestion_mode=(
                "incremental"
            ),
            requested_start_date=(
                start_date.isoformat()
            ),
            requested_end_date=(
                end_date.isoformat()
            ),
        )