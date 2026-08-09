"""
Ingestion logic for REE / ESIOS data.
"""

from datetime import date
from pathlib import Path

from ingestion.common.logger import get_logger
from ingestion.common.storage import LocalBronzeStorage
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
        storage: LocalBronzeStorage | None = None,
    ) -> None:
        self.client = client or EsiosClient()
        self.storage = storage or LocalBronzeStorage()

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
    ) -> Path:
        """
        Retrieve historical values for an ESIOS indicator
        and persist them in Bronze.
        """

        logger.info(
            "Starting ESIOS historical ingestion "
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
            ingestion_mode="historical",
            requested_start_date=start_date.isoformat(),
            requested_end_date=end_date.isoformat(),
        )

        logger.info(
            "ESIOS historical ingestion completed: %s",
            output_path,
        )

        return output_path

    def ingest_incremental(
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
    ) -> Path:
        """
        Retrieve an incremental temporal window for an ESIOS indicator
        and persist it in Bronze.
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