from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession

from silver.aemet import build_aemet_silver
from silver.cnig import build_cnig_silver
from silver.esios import build_esios_silver
from silver.open_meteo import build_open_meteo_silver

from silver.geography import (
    enrich_with_cnig_autonomous_community,
    enrich_with_cnig_province,
    validate_all_autonomous_communities_matched,
    validate_all_provinces_matched,
)

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


# ============================================================================
# Execution modes
# ============================================================================

MODE_ALL = "all"
MODE_GEOGRAPHY_FIX = "geography-fix"
MODE_ESIOS_GEOGRAPHY_FIX = "esios-geography-fix"

VALID_MODES = {
    MODE_ALL,
    MODE_GEOGRAPHY_FIX,
    MODE_ESIOS_GEOGRAPHY_FIX,
}


# ============================================================================
# Natural keys approved for Silver
# ============================================================================

KEY_AEMET_STATIONS = [
    "station_id",
]

KEY_AEMET_CURRENT = [
    "station_id",
    "observation_timestamp",
]


KEY_OPEN_METEO = [
    "station_id",
    "observation_timestamp",
]


KEY_CNIG_PROVINCES = [
    "province_code",
]

KEY_CNIG_AUTONOMOUS_COMMUNITIES = [
    "autonomous_community_code",
]

KEY_CNIG_MUNICIPALITIES = [
    "municipality_ine_code",
]


KEY_ESIOS = [
    "indicator_id",
    "esios_geo_id",
    "observation_timestamp",
]


# ============================================================================
# Argument parsing
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Persist Silver DataFrames into Iceberg."
        )
    )

    parser.add_argument(
        "--mode",
        choices=sorted(VALID_MODES),
        default=MODE_ALL,
        help=(
            "'all' writes the complete Silver layer. "
            "'geography-fix' writes only the meteorological "
            "tables affected by canonical province normalization. "
            "'esios-geography-fix' writes only the ESIOS "
            "tables affected by canonical geographical "
            "normalization."
        ),
    )

    return parser.parse_args()


# ============================================================================
# Helpers
# ============================================================================

def table_exists(
    spark: SparkSession,
    table_name: str,
) -> bool:
    return spark.catalog.tableExists(
        table_name
    )


def validate_target_table(
    spark: SparkSession,
    table_name: str,
) -> None:
    """
    4.4 assumes the required Iceberg tables have already been created.
    """
    if not table_exists(
        spark,
        table_name,
    ):
        raise RuntimeError(
            f"Required Silver Iceberg table does not exist: "
            f"{table_name}"
        )


def validate_source_keys(
    df: DataFrame,
    natural_key: list[str],
    table_name: str,
) -> None:
    """
    Prevent records with NULL natural keys from being written to Silver.
    """
    missing_columns = [
        column
        for column in natural_key
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing natural-key columns for {table_name}: "
            f"{missing_columns}"
        )

    condition = " OR ".join(
        f"`{column}` IS NULL"
        for column in natural_key
    )

    null_key_count = (
        df
        .filter(condition)
        .count()
    )

    if null_key_count != 0:
        raise ValueError(
            f"Cannot write {table_name}: "
            f"{null_key_count} rows contain NULL natural keys."
        )


def merge_into_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    natural_key: list[str],
    view_name: str,
) -> None:
    """
    Idempotent Iceberg upsert.

    The source DataFrame is materialized before MERGE so that expressions
    derived from source-file metadata, such as input_file_name(), do not
    remain as non-deterministic expressions in the MERGE logical plan.

    Existing natural key:
        update the Silver row with the current normalized source row.

    New natural key:
        insert it.
    """

    validate_target_table(
        spark,
        table_name,
    )

    validate_source_keys(
        df,
        natural_key,
        table_name,
    )

    materialized_df = df.localCheckpoint(
        eager=True
    )

    source_count = (
        materialized_df
        .count()
    )

    print("=" * 80)
    print(
        f"TABLE = {table_name}"
    )
    print(
        f"SOURCE_ROWS = {source_count}"
    )

    materialized_df.createOrReplaceTempView(
        view_name
    )

    merge_condition = " AND ".join(
        f"target.`{column}` = source.`{column}`"
        for column in natural_key
    )

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS source
        ON {merge_condition}

        WHEN MATCHED THEN
            UPDATE SET *

        WHEN NOT MATCHED THEN
            INSERT *
        """
    )

    target_count = (
        spark
        .table(table_name)
        .count()
    )

    print(
        f"TARGET_ROWS_AFTER_MERGE = {target_count}"
    )

    print(
        f"MERGED = {table_name}"
    )


# ============================================================================
# Main Silver persistence
# ============================================================================

def main() -> None:
    args = parse_args()
    mode = args.mode

    spark = (
        SparkSession.builder
        .appName(
            "write-silver-iceberg"
        )
        .getOrCreate()
    )

    print("=" * 80)
    print("WRITE SILVER DATA TO ICEBERG")
    print(f"MODE = {mode}")
    print("=" * 80)

    (
        cnig_provinces,
        cnig_autonomous_communities,
        cnig_municipalities,
    ) = build_cnig_silver(
        spark
    )

    if mode == MODE_ESIOS_GEOGRAPHY_FIX:
        (
            esios_energy_hourly,
            esios_installed_capacity,
        ) = build_esios_silver(
            spark
        )

        esios_energy_hourly = enrich_with_cnig_province(
            esios_energy_hourly,
            cnig_provinces,
            source_province_column="esios_geo_name",
        )

        validate_all_provinces_matched(
            esios_energy_hourly,
            dataset_name="silver_esios_energy_hourly",
        )

        esios_installed_capacity = (
            enrich_with_cnig_autonomous_community(
                esios_installed_capacity,
                cnig_autonomous_communities,
                source_autonomous_community_column="esios_geo_name",
            )
        )

        validate_all_autonomous_communities_matched(
            esios_installed_capacity,
            dataset_name="silver_esios_installed_capacity_monthly",
        )

        merge_into_table(
            spark=spark,
            df=esios_energy_hourly,
            table_name=TABLE_ESIOS_ENERGY_HOURLY,
            natural_key=KEY_ESIOS,
            view_name="src_esios_energy_hourly",
        )

        merge_into_table(
            spark=spark,
            df=esios_installed_capacity,
            table_name=TABLE_ESIOS_INSTALLED_CAPACITY,
            natural_key=KEY_ESIOS,
            view_name="src_esios_installed_capacity",
        )

        print("=" * 80)
        print(
            "ESIOS GEOGRAPHY FIX SILVER WRITE COMPLETE"
        )
        print("=" * 80)

        spark.stop()
        return

    (
        aemet_stations,
        aemet_current,
    ) = build_aemet_silver(
        spark
    )

    (
        open_meteo_hourly,
        open_meteo_15min,
    ) = build_open_meteo_silver(
        spark
    )

    aemet_stations = enrich_with_cnig_province(
        aemet_stations,
        cnig_provinces,
        source_province_column="provincia",
    )

    open_meteo_hourly = enrich_with_cnig_province(
        open_meteo_hourly,
        cnig_provinces,
        source_province_column="province",
    )

    open_meteo_15min = enrich_with_cnig_province(
        open_meteo_15min,
        cnig_provinces,
        source_province_column="province",
    )

    validate_all_provinces_matched(
        aemet_stations,
        dataset_name="silver_aemet_stations",
    )

    validate_all_provinces_matched(
        open_meteo_hourly,
        dataset_name="silver_open_meteo_hourly",
    )

    validate_all_provinces_matched(
        open_meteo_15min,
        dataset_name="silver_open_meteo_15min",
    )

    if mode == MODE_GEOGRAPHY_FIX:
        merge_into_table(
            spark=spark,
            df=aemet_stations,
            table_name=TABLE_AEMET_STATIONS,
            natural_key=KEY_AEMET_STATIONS,
            view_name="src_aemet_stations",
        )

        merge_into_table(
            spark=spark,
            df=open_meteo_hourly,
            table_name=TABLE_OPEN_METEO_HOURLY,
            natural_key=KEY_OPEN_METEO,
            view_name="src_open_meteo_hourly",
        )

        merge_into_table(
            spark=spark,
            df=open_meteo_15min,
            table_name=TABLE_OPEN_METEO_15MIN,
            natural_key=KEY_OPEN_METEO,
            view_name="src_open_meteo_15min",
        )

        print("=" * 80)
        print(
            "GEOGRAPHY FIX SILVER WRITE COMPLETE"
        )
        print("=" * 80)

        spark.stop()
        return

    (
        esios_energy_hourly,
        esios_installed_capacity,
    ) = build_esios_silver(
        spark
    )

    esios_energy_hourly = enrich_with_cnig_province(
        esios_energy_hourly,
        cnig_provinces,
        source_province_column="esios_geo_name",
    )

    validate_all_provinces_matched(
        esios_energy_hourly,
        dataset_name="silver_esios_energy_hourly",
    )

    esios_installed_capacity = (
        enrich_with_cnig_autonomous_community(
            esios_installed_capacity,
            cnig_autonomous_communities,
            source_autonomous_community_column="esios_geo_name",
        )
    )

    validate_all_autonomous_communities_matched(
        esios_installed_capacity,
        dataset_name="silver_esios_installed_capacity_monthly",
    )

    merge_into_table(
        spark=spark,
        df=aemet_stations,
        table_name=TABLE_AEMET_STATIONS,
        natural_key=KEY_AEMET_STATIONS,
        view_name="src_aemet_stations",
    )

    merge_into_table(
        spark=spark,
        df=aemet_current,
        table_name=TABLE_AEMET_CURRENT,
        natural_key=KEY_AEMET_CURRENT,
        view_name="src_aemet_current",
    )

    merge_into_table(
        spark=spark,
        df=open_meteo_hourly,
        table_name=TABLE_OPEN_METEO_HOURLY,
        natural_key=KEY_OPEN_METEO,
        view_name="src_open_meteo_hourly",
    )

    merge_into_table(
        spark=spark,
        df=open_meteo_15min,
        table_name=TABLE_OPEN_METEO_15MIN,
        natural_key=KEY_OPEN_METEO,
        view_name="src_open_meteo_15min",
    )

    merge_into_table(
        spark=spark,
        df=cnig_provinces,
        table_name=TABLE_CNIG_PROVINCES,
        natural_key=KEY_CNIG_PROVINCES,
        view_name="src_cnig_provinces",
    )

    merge_into_table(
        spark=spark,
        df=cnig_autonomous_communities,
        table_name=TABLE_CNIG_AUTONOMOUS_COMMUNITIES,
        natural_key=KEY_CNIG_AUTONOMOUS_COMMUNITIES,
        view_name="src_cnig_autonomous_communities",
    )

    merge_into_table(
        spark=spark,
        df=cnig_municipalities,
        table_name=TABLE_CNIG_MUNICIPALITIES,
        natural_key=KEY_CNIG_MUNICIPALITIES,
        view_name="src_cnig_municipalities",
    )

    merge_into_table(
        spark=spark,
        df=esios_energy_hourly,
        table_name=TABLE_ESIOS_ENERGY_HOURLY,
        natural_key=KEY_ESIOS,
        view_name="src_esios_energy_hourly",
    )

    merge_into_table(
        spark=spark,
        df=esios_installed_capacity,
        table_name=TABLE_ESIOS_INSTALLED_CAPACITY,
        natural_key=KEY_ESIOS,
        view_name="src_esios_installed_capacity",
    )

    print("=" * 80)
    print(
        "SILVER ICEBERG WRITE COMPLETE"
    )
    print("=" * 80)

    spark.stop()



if __name__ == "__main__":
    main()