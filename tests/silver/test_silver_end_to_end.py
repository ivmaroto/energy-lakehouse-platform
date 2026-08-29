from pyspark.sql import SparkSession

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


TABLES = [
    (
        "silver_aemet_stations",
        921,
    ),
    (
        "silver_aemet_current_observations",
        9688,
    ),
    (
        "silver_open_meteo_hourly",
        88416,
    ),
    (
        "silver_open_meteo_15min",
        353664,
    ),
    (
        "silver_cnig_provinces",
        52,
    ),
    (
        "silver_cnig_autonomous_communities",
        19,
    ),
    (
        "silver_cnig_municipalities",
        8132,
    ),
    (
        "silver_esios_energy_hourly",
        25689,
    ),
    (
        "silver_esios_installed_capacity_monthly",
        123,
    ),
]



def main():
    spark = (
        SparkSession.builder
        .appName(
            "silver-end-to-end-validation"
        )
        .getOrCreate()
    )

    print("=" * 80)
    print("SILVER END-TO-END VALIDATION")
    print("=" * 80)

    print(
        "STEP 1 = Bronze real -> PySpark Silver"
    )

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

    (
        cnig_provinces,
        cnig_autonomous_communities,
        cnig_municipalities,
    ) = build_cnig_silver(
        spark
    )

    (
        esios_energy_hourly,
        esios_installed_capacity,
    ) = build_esios_silver(
        spark
    )


    # ------------------------------------------------------------------
    # Canonical geographical normalization
    #
    # Must reproduce write_silver.py before comparing the
    # transformed Bronze source with persisted Iceberg.
    # ------------------------------------------------------------------

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

    esios_energy_hourly = enrich_with_cnig_province(
        esios_energy_hourly,
        cnig_provinces,
        source_province_column="esios_geo_name",
    )

    esios_installed_capacity = (
        enrich_with_cnig_autonomous_community(
            esios_installed_capacity,
            cnig_autonomous_communities,
            source_autonomous_community_column=(
                "esios_geo_name"
            ),
        )
    )

    # Same geographical validations used by production.

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

    validate_all_provinces_matched(
        esios_energy_hourly,
        dataset_name="silver_esios_energy_hourly",
    )

    validate_all_autonomous_communities_matched(
        esios_installed_capacity,
        dataset_name=(
            "silver_esios_installed_capacity_monthly"
        ),
    )

    source_dataframes = {
        "silver_aemet_stations":
            aemet_stations,

        "silver_aemet_current_observations":
            aemet_current,

        "silver_open_meteo_hourly":
            open_meteo_hourly,

        "silver_open_meteo_15min":
            open_meteo_15min,

        "silver_cnig_provinces":
            cnig_provinces,

        "silver_cnig_autonomous_communities":
            cnig_autonomous_communities,

        "silver_cnig_municipalities":
            cnig_municipalities,

        "silver_esios_energy_hourly":
            esios_energy_hourly,

        "silver_esios_installed_capacity_monthly":
            esios_installed_capacity,
    }

    print("=" * 80)
    print(
        "STEP 2 = PySpark Silver -> "
        "Iceberg persisted tables"
    )
    print("=" * 80)

    for table_name, expected_rows in TABLES:
        source_df = source_dataframes[
            table_name
        ]

        source_rows = source_df.count()

        full_table_name = (
            f"lakehouse.silver.{table_name}"
        )

        persisted_df = spark.table(
            full_table_name
        )

        persisted_rows = (
            persisted_df.count()
        )

        source_columns = (
            source_df.columns
        )

        persisted_columns = (
            persisted_df.columns
        )

        count_match = (
            source_rows == persisted_rows
        )

        expected_match = (
            persisted_rows == expected_rows
        )

        source_schema = {
            field.name: field.dataType.simpleString()
            for field in source_df.schema.fields
        }

        persisted_schema = {
            field.name: field.dataType.simpleString()
            for field in persisted_df.schema.fields
        }

        column_order_match = (
            source_columns
            == persisted_columns
        )

        column_set_match = (
            set(source_columns)
            == set(persisted_columns)
        )

        schema_match = (
            source_schema
            == persisted_schema
        )

        source_only_columns = sorted(
            set(source_columns)
            - set(persisted_columns)
        )

        target_only_columns = sorted(
            set(persisted_columns)
            - set(source_columns)
        )

        print("-" * 80)
        print(
            f"TABLE = {table_name}"
        )
        print(
            "BRONZE_TO_SILVER_ROWS =",
            source_rows,
        )
        print(
            "ICEBERG_ROWS =",
            persisted_rows,
        )
        print(
            "EXPECTED_ROWS =",
            expected_rows,
        )
        print(
            "SOURCE_TARGET_COUNT_MATCH =",
            count_match,
        )
        print(
            "EXPECTED_COUNT_MATCH =",
            expected_match,
        )
        print(
            "COLUMN_ORDER_MATCH =",
            column_order_match,
        )
        print(
            "COLUMN_SET_MATCH =",
            column_set_match,
        )
        print(
            "SCHEMA_NAME_TYPE_MATCH =",
            schema_match,
        )
        print(
            "SOURCE_ONLY_COLUMNS =",
            source_only_columns,
        )
        print(
            "TARGET_ONLY_COLUMNS =",
            target_only_columns,
        )

        if not count_match:
            raise RuntimeError(
                f"Source/target count mismatch: "
                f"{table_name}"
            )

        if not expected_match:
            raise RuntimeError(
                f"Unexpected persisted count: "
                f"{table_name}"
            )

        if not column_set_match:
            raise RuntimeError(
                f"Source/target column-set mismatch: "
                f"{table_name}"
            )

        if not schema_match:
            raise RuntimeError(
                f"Source/target schema type mismatch: "
                f"{table_name}"
            )

    print("=" * 80)
    print(
        "STEP 3 = Real SQL queries "
        "against Iceberg Silver"
    )
    print("=" * 80)

    for table_name, expected_rows in TABLES:
        full_table_name = (
            f"lakehouse.silver.{table_name}"
        )

        result = spark.sql(
            f"""
            SELECT COUNT(*) AS rows
            FROM {full_table_name}
            """
        ).first()

        sql_rows = result["rows"]

        print(
            f"SQL_QUERY {table_name} =",
            sql_rows,
        )

        if sql_rows != expected_rows:
            raise RuntimeError(
                f"SQL validation failed for "
                f"{table_name}"
            )

    print("=" * 80)
    print(
        "SILVER END-TO-END VALIDATION COMPLETE"
    )
    print("=" * 80)

    spark.stop()



if __name__ == "__main__":
    main()