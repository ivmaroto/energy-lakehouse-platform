"""
Ingestion logic for AEMET OpenData.
"""

from pathlib import Path

from ingestion.aemet.client import AemetClient
from ingestion.common.logger import get_logger
from ingestion.common.storage import MinIOBronzeStorage

from datetime import datetime, timezone


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
        and persist it as the canonical Bronze master object.
        """

        logger.info(
            "Starting AEMET station inventory ingestion."
        )

        data = self.client.get_stations()

        object_name = (
            "bronze/aemet/stations/"
            "stations.json"
        )

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=self.DATASET_STATIONS,
            object_name=object_name,
            ingestion_mode="snapshot",
        )

        logger.info(
            "AEMET station inventory ingestion completed: %s",
            output_path,
        )

        return output_path

    def ingest_current_observations(
            self,
    ) -> list[Path | str]:
        """
        Retrieve current conventional observations from AEMET and persist
        one canonical Bronze object per UTC observation day.

        Bronze natural key:
            idema + fint

        Existing daily Bronze data is merged with the new API response so
        the same observation is never persisted twice.
        """

        logger.info(
            "Starting AEMET conventional observations ingestion."
        )

        data = (
            self.client
            .get_current_observations()
        )

        if not isinstance(
                data,
                list,
        ):
            raise ValueError(
                "AEMET current observations response "
                "must be a list."
            )

        observations_by_day: dict[
            str,
            list[dict],
        ] = {}

        for record in data:
            if not isinstance(
                    record,
                    dict,
            ):
                raise ValueError(
                    "AEMET current observation "
                    "must be a JSON object."
                )

            station_id = record.get(
                "idema"
            )

            fint = record.get(
                "fint"
            )

            if not station_id:
                raise ValueError(
                    "AEMET current observation "
                    "without idema."
                )

            if not fint:
                raise ValueError(
                    "AEMET current observation "
                    "without fint."
                )

            try:
                observation_datetime = (
                    datetime.strptime(
                        str(fint),
                        "%Y-%m-%dT%H:%M:%S%z",
                    )
                )

            except ValueError as exc:
                raise ValueError(
                    "Invalid AEMET fint value: "
                    f"{fint}"
                ) from exc

            if (
                    observation_datetime.tzinfo
                    is None
            ):
                raise ValueError(
                    "AEMET fint must include "
                    f"timezone information: {fint}"
                )

            observation_datetime = (
                observation_datetime
                .astimezone(
                    timezone.utc
                )
            )

            observation_date = (
                observation_datetime
                .date()
                .isoformat()
            )

            observations_by_day.setdefault(
                observation_date,
                [],
            ).append(
                record
            )

        output_paths: list[
            Path | str
            ] = []

        for observation_date in sorted(
                observations_by_day
        ):
            year, month, day = (
                observation_date.split(
                    "-"
                )
            )

            object_name = (
                "bronze/aemet/"
                "current_observations/"
                f"year={year}/"
                f"month={month}/"
                f"day={day}/"
                "observations.json"
            )

            merged_by_key: dict[
                tuple[str, str],
                dict,
            ] = {}

            if self.storage.object_exists(
                    object_name
            ):
                existing_payload = (
                    self.storage.read_json(
                        object_name
                    )
                )

                if not isinstance(
                        existing_payload,
                        dict,
                ):
                    raise ValueError(
                        "Invalid existing AEMET "
                        "Bronze wrapper: "
                        f"{object_name}"
                    )

                existing_data = (
                    existing_payload.get(
                        "data"
                    )
                )

                if not isinstance(
                        existing_data,
                        list,
                ):
                    raise ValueError(
                        "Invalid existing AEMET "
                        "Bronze data: "
                        f"{object_name}"
                    )

                for record in existing_data:
                    if not isinstance(
                            record,
                            dict,
                    ):
                        raise ValueError(
                            "Invalid existing AEMET "
                            "observation in "
                            f"{object_name}"
                        )

                    station_id = record.get(
                        "idema"
                    )

                    fint = record.get(
                        "fint"
                    )

                    if not station_id or not fint:
                        raise ValueError(
                            "Existing AEMET Bronze "
                            "observation without "
                            "idema/fint."
                        )

                    merged_by_key[
                        (
                            str(station_id),
                            str(fint),
                        )
                    ] = record

            for record in (
                    observations_by_day[
                        observation_date
                    ]
            ):
                merged_by_key[
                    (
                        str(
                            record["idema"]
                        ),
                        str(
                            record["fint"]
                        ),
                    )
                ] = record

            merged_data = [
                merged_by_key[key]
                for key in sorted(
                    merged_by_key
                )
            ]

            output_path = (
                self.storage.save_json(
                    merged_data,
                    source=self.SOURCE,
                    dataset=(
                        self.DATASET_CURRENT_OBSERVATIONS
                    ),
                    object_name=(
                        object_name
                    ),
                    ingestion_mode=(
                        "incremental"
                    ),
                    requested_start_date=(
                        observation_date
                    ),
                    requested_end_date=(
                        observation_date
                    ),
                    extra_metadata={
                        "observation_date": (
                            observation_date
                        ),
                    },
                )
            )

            output_paths.append(
                output_path
            )

            logger.info(
                "AEMET observations persisted: "
                "date=%s rows=%s object=%s",
                observation_date,
                len(merged_data),
                object_name,
            )

        logger.info(
            "AEMET conventional observations ingestion "
            "completed. %s daily Bronze objects written.",
            len(output_paths),
        )

        return output_paths