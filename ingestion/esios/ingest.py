"""
Ingestion logic for REE / ESIOS data.
"""

from datetime import date, datetime
from pathlib import Path

from ingestion.common.config import ESIOS_HISTORICAL_CHUNK_DAYS
from ingestion.common.date_utils import split_date_range
from ingestion.common.logger import get_logger
from ingestion.common.storage import MinIOBronzeStorage
from ingestion.esios.client import EsiosClient


logger = get_logger(__name__)


class EsiosIngestion:
    """
    Coordinate REE / ESIOS extraction and Bronze persistence.
    """

    SOURCE = "esios"

    def __init__(
        self,
        client: EsiosClient | None = None,
        storage: MinIOBronzeStorage | None = None,
    ) -> None:
        self.client = client or EsiosClient()
        self.storage = storage or MinIOBronzeStorage()

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
        Retrieve historical values for an ESIOS indicator in chunks
        and persist every chunk independently in Bronze.
        """

        chunks = split_date_range(
            start_date=start_date,
            end_date=end_date,
            chunk_days=ESIOS_HISTORICAL_CHUNK_DAYS,
        )

        logger.info(
            "Starting ESIOS historical ingestion "
            "for indicator=%s, period=%s -> %s (%s chunks)",
            indicator_id,
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
                "Processing ESIOS chunk %s/%s: %s -> %s",
                chunk_number,
                len(chunks),
                chunk_start,
                chunk_end,
            )

            data = self.client.get_indicator(
                indicator_id=indicator_id,
                start_date=chunk_start,
                end_date=chunk_end,
                time_trunc=time_trunc,
                time_agg=time_agg,
                geo_ids=geo_ids,
                geo_trunc=geo_trunc,
                geo_agg=geo_agg,
            )

            output_path = self.storage.save_json(
                data,
                source=self.SOURCE,
                dataset=dataset,
                ingestion_mode="historical",
                requested_start_date=chunk_start.isoformat(),
                requested_end_date=chunk_end.isoformat(),
            )

            output_paths.append(output_path)

        logger.info(
            "ESIOS historical ingestion completed. "
            "%s Bronze files generated.",
            len(output_paths),
        )

        return output_paths

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
    ) -> Path | str:
        """
        Retrieve an incremental temporal window for an ESIOS indicator
        and persist it in Bronze.

        Date values can be used for daily windows.
        Datetime values can be used for high-frequency windows
        such as hourly or monthly ingestion.
        """

        logger.info(
            "Starting ESIOS incremental ingestion "
            "for indicator=%s, period=%s -> %s",
            indicator_id,
            start_date,
            end_date,
        )

        data = self.client.get_indicator(
            indicator_id=indicator_id,
            start_date=start_date,
            end_date=end_date,
            time_trunc=time_trunc,
            time_agg=time_agg,
            geo_ids=geo_ids,
            geo_trunc=geo_trunc,
            geo_agg=geo_agg,
        )

        output_path = self.storage.save_json(
            data,
            source=self.SOURCE,
            dataset=dataset,
            ingestion_mode="incremental",
            requested_start_date=start_date.isoformat(),
            requested_end_date=end_date.isoformat(),
        )

        logger.info(
            "ESIOS incremental ingestion completed: %s",
            output_path,
        )

        return output_path