"""
Reusable historical ingestion orchestration.

This module contains no Airflow-specific code.
It can be executed directly from PyCharm and reused by Airflow DAGs.
"""

import argparse
import calendar

from ingestion.common.storage import MinIOBronzeStorage

from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)

from ingestion.aemet.ingest import AemetIngestion
from ingestion.aemet.master import (
    load_aemet_station_locations,
)
from ingestion.cnig.ingest import CnigIngestion
from ingestion.common.esios_config import (
    load_esios_indicators,
)
from ingestion.common.storage import (
    MinIOBronzeStorage,
)
from ingestion.esios.ingest import EsiosIngestion
from ingestion.open_meteo.batch import (
    OpenMeteoBatchIngestion,
)


# ============================================================================
# Date helpers
# ============================================================================

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


def iter_dates(
    start_date: date,
    end_date: date,
):
    """
    Yield every calendar date in the inclusive interval.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    current = start_date

    while current <= end_date:
        yield current
        current += timedelta(
            days=1
        )


def iter_months(
    start_date: date,
    end_date: date,
):
    """
    Yield the first day of every calendar month touched by
    the inclusive analytical interval.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    current = start_date.replace(
        day=1
    )

    final = end_date.replace(
        day=1
    )

    while current <= final:
        yield current

        if current.month == 12:
            current = date(
                current.year + 1,
                1,
                1,
            )
        else:
            current = date(
                current.year,
                current.month + 1,
                1,
            )


# ============================================================================
# Bronze deletion policy
# ============================================================================

def delete_bronze_range(
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    """
    Delete only historical Bronze fact objects affected by
    the requested analytical interval.

    Master datasets are deliberately preserved.

    Deleted datasets:

        Open-Meteo hourly
            -> daily partitions

        Open-Meteo 15-minute
            -> daily partitions

        ESIOS hourly
            -> daily partitions

        ESIOS installed capacity
            -> monthly partitions touched by the requested range

    AEMET station master and CNIG masters are not deleted.
    AEMET current observations are not part of historical reload.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    storage = MinIOBronzeStorage()

    deleted = {
        "open_meteo_hourly": 0,
        "open_meteo_15min": 0,
        "esios_hourly": 0,
        "esios_monthly": 0,
    }

    # ========================================================================
    # Open-Meteo daily historical partitions
    # ========================================================================

    for current_date in iter_dates(
        start_date,
        end_date,
    ):
        daily_partition = (
            f"year={current_date.year:04d}/"
            f"month={current_date.month:02d}/"
            f"day={current_date.day:02d}/"
        )

        deleted[
            "open_meteo_hourly"
        ] += storage.delete_prefix(
            "bronze/open_meteo/"
            "weather_hourly/"
            + daily_partition
        )

        deleted[
            "open_meteo_15min"
        ] += storage.delete_prefix(
            "bronze/open_meteo/"
            "weather_15min/"
            + daily_partition
        )

    # ========================================================================
    # ESIOS hourly daily partitions
    # ========================================================================

    hourly_indicators = (
        load_esios_indicators(
            "hourly"
        )
    )

    for dataset in (
        hourly_indicators.values()
    ):
        for current_date in iter_dates(
            start_date,
            end_date,
        ):
            deleted[
                "esios_hourly"
            ] += storage.delete_prefix(
                f"bronze/esios/{dataset}/"
                f"year={current_date.year:04d}/"
                f"month={current_date.month:02d}/"
                f"day={current_date.day:02d}/"
            )

    # ========================================================================
    # ESIOS monthly partitions
    #
    # Installed-capacity ingestion expands the requested analytical range
    # to complete calendar months. Therefore any touched month is rebuilt.
    # ========================================================================

    monthly_indicators = (
        load_esios_indicators(
            "monthly"
        )
    )

    for dataset in (
        monthly_indicators.values()
    ):
        for current_month in iter_months(
            start_date,
            end_date,
        ):
            deleted[
                "esios_monthly"
            ] += storage.delete_prefix(
                f"bronze/esios/{dataset}/"
                f"year={current_month.year:04d}/"
                f"month={current_month.month:02d}/"
            )

    print(
        "=" * 80
    )

    print(
        "BRONZE RANGE DELETION"
    )

    print(
        f"RANGE = {start_date} -> {end_date}"
    )

    print(
        deleted
    )

    print(
        "=" * 80
    )

    return deleted


def delete_all_bronze() -> int:
    """
    Delete the complete active Bronze layer.

    Master datasets are also removed.

    MinIOBronzeStorage.delete_prefix() contains defensive
    protection for backup_before_reload_* objects.
    """

    storage = MinIOBronzeStorage()

    deleted = storage.delete_prefix(
        "bronze/"
    )

    print(
        "=" * 80
    )

    print(
        "BRONZE FULL DELETION"
    )

    print(
        f"DELETED_OBJECTS = {deleted}"
    )

    print(
        "=" * 80
    )

    return deleted


def delete_all_warehouse_residuals() -> dict[str, int]:
    """
    Remove all physical Silver and Gold warehouse objects
    after the Iceberg tables have been purged.

    This operation is only intended for a complete
    historical reset.
    """

    storage = MinIOBronzeStorage()

    deleted = {
        "silver": storage.delete_warehouse_layer(
            "warehouse/silver/"
        ),
        "gold": storage.delete_warehouse_layer(
            "warehouse/gold/"
        ),
    }

    print(
        "=" * 80
    )

    print(
        "FULL SILVER/GOLD PHYSICAL CLEANUP"
    )

    print(
        f"DELETED_OBJECTS = {deleted}"
    )

    print(
        "=" * 80
    )

    return deleted


# ============================================================================
# Master ingestion
# ============================================================================

def ingest_masters() -> dict[str, int]:
    """
    Ensure the approved master-data sources exist in Bronze.

    Persistence policy:

        Existing master
            -> preserve it without rewriting.

        Missing master
            -> ingest it.

    This allows:

        normal/preserve load
            -> existing masters remain untouched

        range overwrite
            -> existing masters remain untouched

        full deletion / clean installation
            -> deleted or missing masters are rebuilt

    AEMET stations are required before Open-Meteo because their
    coordinates are the Open-Meteo location master.
    """

    storage = MinIOBronzeStorage()

    # ========================================================================
    # AEMET station master
    # ========================================================================

    aemet_station_object = (
        "bronze/aemet/stations/stations.json"
    )

    aemet_ingested = 0

    if storage.object_exists(
        aemet_station_object
    ):
        print(
            "MASTER PRESERVED = "
            f"{aemet_station_object}"
        )
    else:
        AemetIngestion().ingest_stations()

        aemet_ingested = 1

        print(
            "MASTER INGESTED = "
            f"{aemet_station_object}"
        )

    # ========================================================================
    # CNIG master
    # ========================================================================

    cnig_provinces_object = (
        "bronze/cnig/provinces/provinces.csv"
    )

    cnig_municipalities_object = (
        "bronze/cnig/municipalities/municipalities.csv"
    )

    cnig_complete = (
        storage.object_exists(
            cnig_provinces_object
        )
        and
        storage.object_exists(
            cnig_municipalities_object
        )
    )

    cnig_file_count = 0

    if cnig_complete:
        print(
            "MASTER PRESERVED = "
            f"{cnig_provinces_object}"
        )

        print(
            "MASTER PRESERVED = "
            f"{cnig_municipalities_object}"
        )
    else:
        cnig_paths = (
            CnigIngestion()
            .ingest_ngmep()
        )

        cnig_file_count = len(
            cnig_paths
        )

        print(
            "CNIG MASTERS INGESTED = "
            f"{cnig_file_count}"
        )

    return {
        "aemet_stations": aemet_ingested,
        "cnig_files": cnig_file_count,
    }


# ============================================================================
# ESIOS historical ingestion
# ============================================================================

def ingest_esios_hourly(
    start_date: date,
    end_date: date,
) -> int:
    """
    Ingest every approved hourly ESIOS indicator.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    indicators = (
        load_esios_indicators(
            "hourly"
        )
    )

    ingestion = EsiosIngestion()

    file_count = 0

    for (
        indicator_id,
        dataset,
    ) in indicators.items():
        paths = (
            ingestion
            .ingest_historical(
                indicator_id=indicator_id,
                dataset=dataset,
                start_date=start_date,
                end_date=end_date,
            )
        )

        if isinstance(
            paths,
            (list, tuple),
        ):
            file_count += len(
                paths
            )
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

    validate_date_range(
        start_date,
        end_date,
    )

    monthly_start, monthly_end = (
        get_monthly_range(
            start_date,
            end_date,
        )
    )

    indicators = (
        load_esios_indicators(
            "monthly"
        )
    )

    ingestion = EsiosIngestion()

    file_count = 0

    for (
        indicator_id,
        dataset,
    ) in indicators.items():
        paths = (
            ingestion
            .ingest_historical(
                indicator_id=indicator_id,
                dataset=dataset,
                start_date=monthly_start,
                end_date=monthly_end,
            )
        )

        if isinstance(
            paths,
            (list, tuple),
        ):
            file_count += len(
                paths
            )
        else:
            file_count += 1

    return file_count


# ============================================================================
# Open-Meteo historical ingestion
# ============================================================================

def ingest_open_meteo(
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    """
    Ingest historical Open-Meteo hourly and 15-minute data
    for the complete AEMET station master.

    Historical Bronze storage is physically organized by
    observation date and station.

    With resume=True, already complete daily station objects
    are reused and only missing daily partitions are downloaded.
    """

    validate_date_range(
        start_date,
        end_date,
    )

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

    # ========================================================================
    # Hourly historical weather
    # ========================================================================

    hourly_paths = (
        ingestion
        .ingest_hourly_range_locations(
            locations=locations,
            start_date=start_date,
            end_date=end_date,
            resume=True,
        )
    )

    # ========================================================================
    # 15-minute historical weather
    # ========================================================================

    interval_start = (
        datetime.combine(
            start_date,
            time.min,
            tzinfo=timezone.utc,
        )
    )

    interval_end = (
        datetime.combine(
            end_date,
            time.max,
            tzinfo=timezone.utc,
        )
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
        "locations": len(
            locations
        ),
        "hourly_files": len(
            hourly_paths
        ),
        "minutely_15_files": len(
            minutely_paths
        ),
    }


# ============================================================================
# AEMET current ingestion
# ============================================================================

def ingest_aemet_current() -> int:
    """
    Ingest AEMET current observations once.

    This function is intentionally kept for the incremental/hourly
    orchestration.

    It is NOT part of historical reload because AEMET current
    observations cannot reconstruct an arbitrary historical interval.
    """

    paths = (
        AemetIngestion()
        .ingest_current_observations()
    )

    if isinstance(
        paths,
        (list, tuple),
    ):
        return len(
            paths
        )

    return 1


# ============================================================================
# Complete Bronze historical reload
# ============================================================================

def run_bronze_historical_reload(
    *,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """
    Execute the complete historical Bronze ingestion workflow.

    This function performs ingestion only.

    Range overwrite and complete-deletion policy are exposed separately
    through delete_bronze_range() and delete_all_bronze() so that Airflow
    can explicitly decide which persistence policy must run.

    AEMET current observations are deliberately excluded.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    print(
        "=" * 80
    )

    print(
        f"HISTORICAL BRONZE LOAD: "
        f"{start_date} -> {end_date}"
    )

    print(
        "=" * 80
    )

    # ========================================================================
    # Masters
    # ========================================================================

    masters = ingest_masters()

    # ========================================================================
    # Historical energy
    # ========================================================================

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

    # ========================================================================
    # Historical meteorology
    # ========================================================================

    open_meteo = (
        ingest_open_meteo(
            start_date,
            end_date,
        )
    )

    # ========================================================================
    # Result
    # ========================================================================

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
    }

    print(
        "=" * 80
    )

    print(
        "BRONZE HISTORICAL LOAD COMPLETED"
    )

    print(
        result
    )

    print(
        "=" * 80
    )

    return result


# ============================================================================
# CLI
# ============================================================================

def main() -> None:
    parser = (
        argparse.ArgumentParser(
            description=(
                "Run historical Bronze ingestion "
                "for the Energy Lakehouse Platform."
            )
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