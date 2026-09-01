from __future__ import annotations

import argparse

from datetime import date

from pyspark.sql import SparkSession

from silver.create_tables import (
    TABLE_AEMET_CURRENT,
    TABLE_AEMET_STATIONS,
    TABLE_CNIG_AUTONOMOUS_COMMUNITIES,
    TABLE_CNIG_MUNICIPALITIES,
    TABLE_CNIG_PROVINCES,
    TABLE_ESIOS_ENERGY_HOURLY,
    TABLE_ESIOS_INSTALLED_CAPACITY,
    TABLE_OPEN_METEO_15MIN,
    TABLE_OPEN_METEO_HOURLY,
)

from gold.common import (
    TABLE_GOLD_DIM_GEOGRAPHY,
    TABLE_GOLD_DIM_TIME,
    TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY,
    TABLE_GOLD_FACT_PROVINCE_HOURLY,
)


MODE_RANGE = "range"
MODE_FULL = "full"

VALID_MODES = {
    MODE_RANGE,
    MODE_FULL,
}


SILVER_TABLES = [
    TABLE_AEMET_STATIONS,
    TABLE_AEMET_CURRENT,
    TABLE_OPEN_METEO_HOURLY,
    TABLE_OPEN_METEO_15MIN,
    TABLE_CNIG_PROVINCES,
    TABLE_CNIG_AUTONOMOUS_COMMUNITIES,
    TABLE_CNIG_MUNICIPALITIES,
    TABLE_ESIOS_ENERGY_HOURLY,
    TABLE_ESIOS_INSTALLED_CAPACITY,
]


GOLD_TABLES = [
    TABLE_GOLD_FACT_PROVINCE_HOURLY,
    TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY,
    TABLE_GOLD_DIM_TIME,
    TABLE_GOLD_DIM_GEOGRAPHY,
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Delete historical Silver and Gold data "
            "before an explicit reload."
        )
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=sorted(
            VALID_MODES
        ),
    )

    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
    )

    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
    )

    return parser.parse_args()


def validate_date_range(
    start_date: date,
    end_date: date,
) -> None:
    if start_date > end_date:
        raise ValueError(
            "start_date cannot be after end_date."
        )


def validate_args(
    args: argparse.Namespace,
) -> None:
    if args.mode == MODE_RANGE:
        if (
            args.start_date is None
            or args.end_date is None
        ):
            raise ValueError(
                "start-date and end-date are "
                "required in range mode."
            )

        validate_date_range(
            args.start_date,
            args.end_date,
        )


def month_bounds(
    start_date: date,
    end_date: date,
) -> tuple[str, str, date, date]:
    """
    Return:

        first year-month
        last year-month
        first calendar day
        exclusive first day after the final month
    """

    first_month = (
        start_date.replace(
            day=1
        )
    )

    final_month = (
        end_date.replace(
            day=1
        )
    )

    if final_month.month == 12:
        next_month = date(
            final_month.year + 1,
            1,
            1,
        )
    else:
        next_month = date(
            final_month.year,
            final_month.month + 1,
            1,
        )

    first_year_month = (
        first_month.strftime(
            "%Y-%m"
        )
    )

    final_year_month = (
        final_month.strftime(
            "%Y-%m"
        )
    )

    return (
        first_year_month,
        final_year_month,
        first_month,
        next_month,
    )


def table_exists(
    spark: SparkSession,
    table_name: str,
) -> bool:
    return spark.catalog.tableExists(
        table_name
    )


def execute_delete(
    spark: SparkSession,
    *,
    table_name: str,
    condition: str,
) -> None:
    if not table_exists(
        spark,
        table_name,
    ):
        print(
            f"SKIP_MISSING_TABLE = {table_name}"
        )
        return

    before = (
        spark
        .table(
            table_name
        )
        .count()
    )

    spark.sql(
        f"""
        DELETE FROM {table_name}
        WHERE {condition}
        """
    )

    after = (
        spark
        .table(
            table_name
        )
        .count()
    )

    print(
        f"TABLE = {table_name}"
    )

    print(
        f"ROWS_BEFORE = {before}"
    )

    print(
        f"ROWS_AFTER = {after}"
    )

    print(
        f"ROWS_DELETED = {before - after}"
    )


def execute_drop_purge(
    spark: SparkSession,
    *,
    table_name: str,
) -> None:
    if not table_exists(
        spark,
        table_name,
    ):
        print(
            f"SKIP_MISSING_TABLE = {table_name}"
        )
        return

    before = (
        spark
        .table(
            table_name
        )
        .count()
    )

    spark.sql(
        f"""
        DROP TABLE {table_name} PURGE
        """
    )

    exists_after = table_exists(
        spark,
        table_name,
    )

    print(
        f"TABLE = {table_name}"
    )

    print(
        f"ROWS_BEFORE = {before}"
    )

    print(
        f"TABLE_EXISTS_AFTER = {exists_after}"
    )

    if exists_after:
        raise RuntimeError(
            f"Table still exists after PURGE: {table_name}"
        )


def delete_all(
    spark: SparkSession,
) -> None:
    """
    Drop and physically purge the complete approved
    Silver and Gold Iceberg model.

    Tables are recreated later by the normal
    Silver/Gold create-table tasks.
    """

    print(
        "=" * 80
    )

    print(
        "FULL SILVER/GOLD PURGE"
    )

    print(
        "=" * 80
    )

    # Gold first because it is derived from Silver.
    for table_name in GOLD_TABLES:
        execute_drop_purge(
            spark,
            table_name=table_name,
        )

    for table_name in SILVER_TABLES:
        execute_drop_purge(
            spark,
            table_name=table_name,
        )

    print(
        "=" * 80
    )

    print(
        "FULL SILVER/GOLD PURGE COMPLETED"
    )

    print(
        "=" * 80
    )


def delete_range(
    spark: SparkSession,
    *,
    start_date: date,
    end_date: date,
) -> None:
    """
    Delete only historical analytical data affected by the
    requested interval.

    Preserved in range mode:

        Silver AEMET station master
        Silver AEMET current observations
        Silver CNIG masters
        Gold geography dimension

    ESIOS installed capacity and monthly Gold members are
    deleted for every calendar month touched by the range.
    """

    validate_date_range(
        start_date,
        end_date,
    )

    (
        first_year_month,
        final_year_month,
        first_month,
        next_month,
    ) = month_bounds(
        start_date,
        end_date,
    )

    start_text = (
        start_date.isoformat()
    )

    end_text = (
        end_date.isoformat()
    )

    first_month_text = (
        first_month.isoformat()
    )

    next_month_text = (
        next_month.isoformat()
    )

    print(
        "=" * 80
    )

    print(
        "RANGE SILVER/GOLD DELETION"
    )

    print(
        f"RANGE = {start_text} -> {end_text}"
    )

    print(
        "MONTH_RANGE = "
        f"{first_year_month} -> {final_year_month}"
    )

    print(
        "=" * 80
    )

    # ========================================================================
    # Gold
    # ========================================================================

    execute_delete(
        spark,
        table_name=(
            TABLE_GOLD_FACT_PROVINCE_HOURLY
        ),
        condition=(
            "CAST(gold_timestamp AS DATE) "
            f"BETWEEN DATE '{start_text}' "
            f"AND DATE '{end_text}'"
        ),
    )

    execute_delete(
        spark,
        table_name=(
            TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY
        ),
        condition=(
            f"year_month >= '{first_year_month}' "
            f"AND year_month <= '{final_year_month}'"
        ),
    )

    # Remove affected temporal members.
    # Geography is a master-style dimension and is preserved.
    execute_delete(
        spark,
        table_name=TABLE_GOLD_DIM_TIME,
        condition=(
            "("
            "time_grain = 'HOUR' "
            "AND CAST(gold_timestamp AS DATE) "
            f"BETWEEN DATE '{start_text}' "
            f"AND DATE '{end_text}'"
            ") "
            "OR "
            "("
            "time_grain = 'MONTH' "
            f"AND year_month >= '{first_year_month}' "
            f"AND year_month <= '{final_year_month}'"
            ")"
        ),
    )

    # ========================================================================
    # Silver historical facts
    # ========================================================================

    for table_name in [
        TABLE_OPEN_METEO_HOURLY,
        TABLE_OPEN_METEO_15MIN,
        TABLE_ESIOS_ENERGY_HOURLY,
    ]:
        execute_delete(
            spark,
            table_name=table_name,
            condition=(
                "CAST(observation_timestamp AS DATE) "
                f"BETWEEN DATE '{start_text}' "
                f"AND DATE '{end_text}'"
            ),
        )

    # Installed capacity is rebuilt by complete touched month.
    execute_delete(
        spark,
        table_name=(
            TABLE_ESIOS_INSTALLED_CAPACITY
        ),
        condition=(
            "observation_timestamp "
            f">= TIMESTAMP '{first_month_text} 00:00:00' "
            "AND observation_timestamp "
            f"< TIMESTAMP '{next_month_text} 00:00:00'"
        ),
    )

    print(
        "=" * 80
    )

    print(
        "RANGE SILVER/GOLD DELETION COMPLETED"
    )

    print(
        "=" * 80
    )


def main() -> None:
    args = parse_args()

    validate_args(
        args
    )

    spark = (
        SparkSession.builder
        .appName(
            "historical-data-deletion"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        if args.mode == MODE_FULL:
            delete_all(
                spark
            )

        else:
            delete_range(
                spark,
                start_date=args.start_date,
                end_date=args.end_date,
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
