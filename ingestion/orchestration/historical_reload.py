"""
Reusable historical ingestion orchestration.

This module contains no Airflow-specific code.
It can be executed directly from PyCharm and reused by Airflow DAGs.
"""

import argparse
import calendar

from datetime import date, datetime, time, timezone

from ingestion.aemet.ingest import AemetIngestion
from ingestion.aemet.master import load_aemet_station_locations
from ingestion.cnig.ingest import CnigIngestion
from ingestion.common.esios_config import load_esios_indicators
from ingestion.esios.ingest import EsiosIngestion
from ingestion.open_meteo.batch import OpenMeteoBatchIngestion


def validate_date_range(
    start_date: date,
    end_date: date,
) -> None:
    if start_date > end_date:
        raise ValueError(
            "start_date cannot be after end_date."
        )


def get_monthly_range(
    start_date: date,
    end_date: date,
) -> tuple[date, date]:
    """
    Expand an analytical date range to complete calendar months
    for the ESIOS installed-capacity datasets.
    """

    monthly_start = start_date.replace(
        day=1
    )

    last_day = calendar.monthrange(
        end_date.year,
        end_date.month,
    )[1]

    monthly_end = end_date.replace(
        day=last_day
    )

    return monthly_start, monthly_end


def ingest_masters() -> dict[str, int]:
    """
    Refresh the two approved master-data sources.

    AEMET stations are ingested before Open-Meteo because their
    coordinates are the Open-Meteo location master.
    """

    AemetIngestion().ingest_stations()

    cnig_paths = (
        CnigIngestion()
        .ingest_ngmep()
    )

    return {
        "aemet_stations": 1,
        "cnig_files": len(cnig_paths),
    }


def ingest_esios_hourly(
    start_date: date,
    end_date: date,
) -> int:
    """
    Ingest every approved hourly ESIOS indicator.
    """

    indicators = load_esios_indicators(
        "hourly"
    )

    ingestion = EsiosIngestion()

    file_count = 0

    for indicator_id, dataset in (
        indicators.items()
    ):
        paths = ingestion.ingest_historical(
            indicator_id=indicator_id,
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
        )

        if isinstance(
            paths,
            (list, tuple),
        ):
            file_count += len(paths)
        else:
            file_count += 1

    return file_count


def ingest_esios_monthly(
    start_date: date,
    end_date: date,
) -> int:
    """
    Ingest every approved monthly installed-capacity indicator.
    """

    monthly_start, monthly_end = (
        get_monthly_range(
            start_date,
            end_date,
        )
    )

    indicators = load_esios_indicators(
        "monthly"
    )

    ingestion = EsiosIngestion()

    file_count = 0

    for indicator_id, dataset in (
        indicators.items()
    ):
        paths = ingestion.ingest_historical(
            indicator_id=indicator_id,
            dataset=dataset,
            start_date=monthly_start,
            end_date=monthly_end,
        )

        if isinstance(
            paths,
            (list, tuple),
        ):
            file_count += len(paths)
        else:
            file_count += 1

    return file_count


def ingest_open_meteo(
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    """
    Ingest historical Open-Meteo hourly and 15-minute data
    for the complete AEMET station master.

    The ingestion is resumable: Bronze objects already complete
    for the exact requested interval are reused and only missing
    station locations are requested again.
    """

    locations = (
        load_aemet_station_locations()
    )

    if not locations:
        raise RuntimeError(
            "AEMET station master returned no "
            "Open-Meteo locations."
        )

    ingestion = (
        OpenMeteoBatchIngestion()
    )

    hourly_paths = (
        ingestion
        .ingest_hourly_range_locations(
            locations=locations,
            start_date=start_date,
            end_date=end_date,
            resume=True,
        )
    )

    interval_start = datetime.combine(
        start_date,
        time.min,
        tzinfo=timezone.utc,
    )

    interval_end = datetime.combine(
        end_date,
        time.max,
        tzinfo=timezone.utc,
    )

    minutely_paths = (
        ingestion
        .ingest_15min_locations(
            locations=locations,
            start_datetime=interval_start,
            end_datetime=interval_end,
            resume=True,
            ingestion_mode="historical",
        )
    )

    return {
        "locations": len(locations),
        "hourly_files": len(
            hourly_paths
        ),
        "minutely_15_files": len(
            minutely_paths
        ),
    }


def ingest_aemet_current() -> int:
    """
    Ingest AEMET current observations once.

    AEMET current observations are not treated as historical
    observations for the requested date range.
    """

    AemetIngestion().ingest_current_observations()

    return 1


def run_bronze_historical_reload(
    *,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """
    Execute the complete historical Bronze ingestion workflow.

    Overwrite / persistence policy is deliberately not implemented
    here. That belongs to the application startup policy.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    print("=" * 80)
    print(
        f"HISTORICAL BRONZE LOAD: "
        f"{start_date} -> {end_date}"
    )
    print("=" * 80)

    masters = ingest_masters()

    esios_hourly_files = (
        ingest_esios_hourly(
            start_date,
            end_date,
        )
    )

    esios_monthly_files = (
        ingest_esios_monthly(
            start_date,
            end_date,
        )
    )

    open_meteo = ingest_open_meteo(
        start_date,
        end_date,
    )

    aemet_current_files = (
        ingest_aemet_current()
    )

    result = {
        "start_date": (
            start_date.isoformat()
        ),
        "end_date": (
            end_date.isoformat()
        ),
        "masters": masters,
        "esios_hourly_files": (
            esios_hourly_files
        ),
        "esios_monthly_files": (
            esios_monthly_files
        ),
        "open_meteo": open_meteo,
        "aemet_current_files": (
            aemet_current_files
        ),
    }

    print("=" * 80)
    print(
        "BRONZE HISTORICAL LOAD COMPLETED"
    )
    print(result)
    print("=" * 80)

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run historical Bronze ingestion "
            "for the Energy Lakehouse Platform."
        )
    )

    parser.add_argument(
        "--start-date",
        required=True,
        type=date.fromisoformat,
    )

    parser.add_argument(
        "--end-date",
        required=True,
        type=date.fromisoformat,
    )

    args = parser.parse_args()

    run_bronze_historical_reload(
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()