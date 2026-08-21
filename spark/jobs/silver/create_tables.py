from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.aemet import build_aemet_silver
from silver.cnig import build_cnig_silver
from silver.esios import build_esios_silver
from silver.open_meteo import build_open_meteo_silver

from silver.geography import (
    enrich_with_cnig_province,
    validate_all_provinces_matched,
)


CATALOG = "lakehouse"
NAMESPACE = "silver"


# ============================================================================
# Iceberg Silver table names
# ============================================================================

TABLE_AEMET_STATIONS = (
    f"{CATALOG}.{NAMESPACE}.silver_aemet_stations"
)

TABLE_AEMET_DAILY = (
    f"{CATALOG}.{NAMESPACE}.silver_aemet_daily_climatology"
)

TABLE_AEMET_CURRENT = (
    f"{CATALOG}.{NAMESPACE}.silver_aemet_current_observations"
)


TABLE_OPEN_METEO_HOURLY = (
    f"{CATALOG}.{NAMESPACE}.silver_open_meteo_hourly"
)

TABLE_OPEN_METEO_HISTORICAL = (
    f"{CATALOG}.{NAMESPACE}.silver_open_meteo_historical_forecast"
)

TABLE_OPEN_METEO_15MIN = (
    f"{CATALOG}.{NAMESPACE}.silver_open_meteo_15min"
)


TABLE_CNIG_PROVINCES = (
    f"{CATALOG}.{NAMESPACE}.silver_cnig_provinces"
)

TABLE_CNIG_AUTONOMOUS_COMMUNITIES = (
    f"{CATALOG}.{NAMESPACE}.silver_cnig_autonomous_communities"
)

TABLE_CNIG_MUNICIPALITIES = (
    f"{CATALOG}.{NAMESPACE}.silver_cnig_municipalities"
)


TABLE_ESIOS_ENERGY_HOURLY = (
    f"{CATALOG}.{NAMESPACE}.silver_esios_energy_hourly"
)

TABLE_ESIOS_POWER_5MIN = (
    f"{CATALOG}.{NAMESPACE}.silver_esios_power_5min"
)

TABLE_ESIOS_INSTALLED_CAPACITY = (
    f"{CATALOG}.{NAMESPACE}."
    "silver_esios_installed_capacity_monthly"
)


# ============================================================================
# Helpers
# ============================================================================

def table_exists(
    spark: SparkSession,
    table_name: str,
) -> bool:
    """
    Check whether an Iceberg table already exists in the configured catalog.
    """
    return spark.catalog.tableExists(
        table_name
    )


def create_unpartitioned_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Create an empty unpartitioned Iceberg table using the schema of the
    validated Silver DataFrame.

    If the table already exists, it is left untouched.
    """
    if table_exists(
        spark,
        table_name,
    ):
        print(
            f"EXISTS = {table_name}"
        )
        return

    (
        df
        .limit(0)
        .writeTo(table_name)
        .using("iceberg")
        .create()
    )

    print(
        f"CREATED = {table_name}"
    )


def create_day_partitioned_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    timestamp_column: str,
) -> None:
    """
    Create an empty Iceberg table partitioned using Iceberg days().

    If the table already exists, it is left untouched.
    """
    if table_exists(
        spark,
        table_name,
    ):
        print(
            f"EXISTS = {table_name}"
        )
        return

    (
        df
        .limit(0)
        .writeTo(table_name)
        .using("iceberg")
        .partitionedBy(
            F.days(
                timestamp_column
            )
        )
        .create()
    )

    print(
        f"CREATED = {table_name}"
    )


def create_month_partitioned_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    temporal_column: str,
) -> None:
    """
    Create an empty Iceberg table partitioned using Iceberg months().

    If the table already exists, it is left untouched.
    """
    if table_exists(
        spark,
        table_name,
    ):
        print(
            f"EXISTS = {table_name}"
        )
        return

    (
        df
        .limit(0)
        .writeTo(table_name)
        .using("iceberg")
        .partitionedBy(
            F.months(
                temporal_column
            )
        )
        .create()
    )

    print(
        f"CREATED = {table_name}"
    )


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    spark = (
        SparkSession.builder
        .appName(
            "create-silver-iceberg-tables"
        )
        .getOrCreate()
    )

    print("=" * 80)
    print(
        "CREATE SILVER ICEBERG TABLES"
    )
    print("=" * 80)

    # ------------------------------------------------------------------------
    # Silver namespace
    # ------------------------------------------------------------------------

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{CATALOG}.{NAMESPACE}"
    )

    print(
        f"NAMESPACE READY = "
        f"{CATALOG}.{NAMESPACE}"
    )

    # ------------------------------------------------------------------------
    # Build validated Silver DataFrames
    #
    # Their schemas are used as the physical schemas of the Iceberg tables.
    # No data is written in this step.
    # ------------------------------------------------------------------------

    (
        aemet_stations,
        aemet_daily,
        aemet_current,
    ) = build_aemet_silver(
        spark
    )

    (
        open_meteo_hourly,
        open_meteo_historical,
        open_meteo_15min,
    ) = build_open_meteo_silver(
        spark
    )

    (
        cnig_provinces,
        cnig_autonomous_communities,
        cnig_municipalities,
    ) = build_cnig_silver(
        spark
    )

    (
        esios_energy_hourly,
        esios_power_5min,
        esios_installed_capacity,
    ) = build_esios_silver(
        spark
    )

    # ------------------------------------------------------------------------
    # Canonical geographical normalization
    #
    # CNIG is the canonical province master.
    #
    # Original source province names remain available for traceability.
    # Canonical identifiers/names are added before deriving the physical
    # Iceberg schemas.
    # ------------------------------------------------------------------------

    aemet_stations = (
        enrich_with_cnig_province(
            aemet_stations,
            cnig_provinces,
            source_province_column="provincia",
        )
    )

    aemet_daily = (
        enrich_with_cnig_province(
            aemet_daily,
            cnig_provinces,
            source_province_column="provincia",
        )
    )

    open_meteo_hourly = (
        enrich_with_cnig_province(
            open_meteo_hourly,
            cnig_provinces,
            source_province_column="province",
        )
    )

    open_meteo_historical = (
        enrich_with_cnig_province(
            open_meteo_historical,
            cnig_provinces,
            source_province_column="province",
        )
    )

    open_meteo_15min = (
        enrich_with_cnig_province(
            open_meteo_15min,
            cnig_provinces,
            source_province_column="province",
        )
    )

    esios_energy_hourly = (
        enrich_with_cnig_province(
            esios_energy_hourly,
            cnig_provinces,
            source_province_column="esios_geo_name",
        )
    )

    # ------------------------------------------------------------------------
    # Validate canonical province resolution
    # ------------------------------------------------------------------------

    validate_all_provinces_matched(
        aemet_stations,
        dataset_name=(
            "silver_aemet_stations"
        ),
    )

    validate_all_provinces_matched(
        aemet_daily,
        dataset_name=(
            "silver_aemet_daily_climatology"
        ),
    )

    validate_all_provinces_matched(
        open_meteo_hourly,
        dataset_name=(
            "silver_open_meteo_hourly"
        ),
    )

    validate_all_provinces_matched(
        open_meteo_historical,
        dataset_name=(
            "silver_open_meteo_historical_forecast"
        ),
    )

    validate_all_provinces_matched(
        open_meteo_15min,
        dataset_name=(
            "silver_open_meteo_15min"
        ),
    )

    validate_all_provinces_matched(
        esios_energy_hourly,
        dataset_name=(
            "silver_esios_energy_hourly"
        ),
    )

    # ------------------------------------------------------------------------
    # AEMET
    # ------------------------------------------------------------------------

    create_unpartitioned_table(
        spark,
        aemet_stations,
        TABLE_AEMET_STATIONS,
    )

    create_month_partitioned_table(
        spark,
        aemet_daily,
        TABLE_AEMET_DAILY,
        "observation_date",
    )

    create_day_partitioned_table(
        spark,
        aemet_current,
        TABLE_AEMET_CURRENT,
        "observation_timestamp",
    )

    # ------------------------------------------------------------------------
    # Open-Meteo
    # ------------------------------------------------------------------------

    create_day_partitioned_table(
        spark,
        open_meteo_hourly,
        TABLE_OPEN_METEO_HOURLY,
        "observation_timestamp",
    )

    create_day_partitioned_table(
        spark,
        open_meteo_historical,
        TABLE_OPEN_METEO_HISTORICAL,
        "observation_timestamp",
    )

    create_day_partitioned_table(
        spark,
        open_meteo_15min,
        TABLE_OPEN_METEO_15MIN,
        "observation_timestamp",
    )

    # ------------------------------------------------------------------------
    # CNIG
    # ------------------------------------------------------------------------

    create_unpartitioned_table(
        spark,
        cnig_provinces,
        TABLE_CNIG_PROVINCES,
    )

    create_unpartitioned_table(
        spark,
        cnig_autonomous_communities,
        TABLE_CNIG_AUTONOMOUS_COMMUNITIES,
    )

    create_unpartitioned_table(
        spark,
        cnig_municipalities,
        TABLE_CNIG_MUNICIPALITIES,
    )

    # ------------------------------------------------------------------------
    # ESIOS
    # ------------------------------------------------------------------------

    create_day_partitioned_table(
        spark,
        esios_energy_hourly,
        TABLE_ESIOS_ENERGY_HOURLY,
        "observation_timestamp",
    )

    create_day_partitioned_table(
        spark,
        esios_power_5min,
        TABLE_ESIOS_POWER_5MIN,
        "observation_timestamp",
    )

    create_month_partitioned_table(
        spark,
        esios_installed_capacity,
        TABLE_ESIOS_INSTALLED_CAPACITY,
        "observation_timestamp",
    )

    print("=" * 80)
    print(
        "SILVER ICEBERG TABLE CREATION COMPLETE"
    )
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()