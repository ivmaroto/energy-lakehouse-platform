from __future__ import annotations

from pyspark.sql import SparkSession

from gold.common import get_spark_session


# ============================================================================
# Gold namespace
# ============================================================================

GOLD_NAMESPACE = "lakehouse.gold"


# ============================================================================
# Approved physical Gold tables
# ============================================================================

GOLD_TABLES = (
    "gold_fact_province_hourly",
    "gold_fact_installed_capacity_monthly",
    "gold_fact_country_15min",
    "gold_fact_country_5min",
    "gold_dim_time",
    "gold_dim_geography",
)


# ============================================================================
# Approved Gold DDL
# ============================================================================

CREATE_GOLD_NAMESPACE_SQL = f"""
CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}
"""


CREATE_GOLD_FACT_PROVINCE_HOURLY_SQL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.gold_fact_province_hourly (
    gold_timestamp TIMESTAMP,
    geography_key STRING,
    province_code STRING,
    province_name STRING,
    autonomous_community_code STRING,
    autonomous_community_name STRING,

    temperature DOUBLE,
    humidity DOUBLE,
    precipitation DOUBLE,

    wind_speed_80m DOUBLE,
    wind_direction_80m DOUBLE,
    wind_speed_120m DOUBLE,
    wind_direction_120m DOUBLE,

    solar_radiation DOUBLE,
    direct_normal_irradiance DOUBLE,

    wind_generation_mwh DOUBLE,
    solar_photovoltaic_generation_mwh DOUBLE,
    solar_thermal_generation_mwh DOUBLE,
    hydraulic_generation_mwh DOUBLE,
    nuclear_generation_mwh DOUBLE,
    combined_cycle_generation_mwh DOUBLE,
    gas_natural_steam_turbine_generation_mwh DOUBLE,
    gas_natural_cogeneration_mwh DOUBLE,
    coal_generation_mwh DOUBLE,
    other_renewables_generation_mwh DOUBLE,
    total_generation_mwh DOUBLE,

    temperature_source STRING,
    humidity_source STRING,
    precipitation_source STRING,

    gold_created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (
    days(gold_timestamp)
)
"""


CREATE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY_SQL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.gold_fact_installed_capacity_monthly (
    year_month STRING,
    gold_month_timestamp TIMESTAMP,
    source_timestamp TIMESTAMP,

    geography_key STRING,
    autonomous_community_code STRING,
    autonomous_community_name STRING,
    esios_geo_id BIGINT,

    hydraulic_installed_capacity_mw DOUBLE,
    wind_installed_capacity_mw DOUBLE,
    solar_photovoltaic_installed_capacity_mw DOUBLE,
    solar_thermal_installed_capacity_mw DOUBLE,
    renewable_total_installed_capacity_mw DOUBLE,
    nuclear_installed_capacity_mw DOUBLE,
    coal_installed_capacity_mw DOUBLE,
    combined_cycle_installed_capacity_mw DOUBLE,
    other_renewables_installed_capacity_mw DOUBLE,

    gold_created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (
    year_month
)
"""


CREATE_GOLD_FACT_COUNTRY_15MIN_SQL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.gold_fact_country_15min (
    gold_timestamp TIMESTAMP,

    geography_key STRING,
    geography_level STRING,
    geography_name STRING,

    temperature DOUBLE,
    humidity DOUBLE,
    precipitation DOUBLE,

    wind_speed_80m DOUBLE,
    wind_direction_80m DOUBLE,
    wind_speed_120m DOUBLE,
    wind_direction_120m DOUBLE,

    solar_radiation DOUBLE,
    direct_normal_irradiance DOUBLE,

    real_demand_energy_mwh_15min DOUBLE,
    wind_generation_energy_mwh_15min DOUBLE,
    nuclear_generation_energy_mwh_15min DOUBLE,
    coal_generation_energy_mwh_15min DOUBLE,
    combined_cycle_generation_energy_mwh_15min DOUBLE,
    hydraulic_generation_energy_mwh_15min DOUBLE,
    solar_photovoltaic_generation_energy_mwh_15min DOUBLE,
    solar_thermal_generation_energy_mwh_15min DOUBLE,
    renewable_thermal_generation_energy_mwh_15min DOUBLE,
    cogeneration_waste_generation_energy_mwh_15min DOUBLE,
    pumping_consumption_energy_mwh_15min DOUBLE,

    gold_created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (
    days(gold_timestamp)
)
"""


CREATE_GOLD_FACT_COUNTRY_5MIN_SQL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.gold_fact_country_5min (
    gold_timestamp TIMESTAMP,

    geography_key STRING,
    geography_level STRING,
    geography_name STRING,
    esios_geo_id BIGINT,

    real_demand_mw DOUBLE,
    wind_generation_power_mw DOUBLE,
    nuclear_generation_power_mw DOUBLE,
    coal_generation_power_mw DOUBLE,
    combined_cycle_generation_power_mw DOUBLE,
    hydraulic_generation_power_mw DOUBLE,
    solar_photovoltaic_generation_power_mw DOUBLE,
    solar_thermal_generation_power_mw DOUBLE,
    renewable_thermal_generation_power_mw DOUBLE,
    cogeneration_waste_generation_power_mw DOUBLE,
    pumping_consumption_power_mw DOUBLE,

    real_demand_energy_mwh_5min DOUBLE,
    wind_generation_energy_mwh_5min DOUBLE,
    nuclear_generation_energy_mwh_5min DOUBLE,
    coal_generation_energy_mwh_5min DOUBLE,
    combined_cycle_generation_energy_mwh_5min DOUBLE,
    hydraulic_generation_energy_mwh_5min DOUBLE,
    solar_photovoltaic_generation_energy_mwh_5min DOUBLE,
    solar_thermal_generation_energy_mwh_5min DOUBLE,
    renewable_thermal_generation_energy_mwh_5min DOUBLE,
    cogeneration_waste_generation_energy_mwh_5min DOUBLE,
    pumping_consumption_energy_mwh_5min DOUBLE,

    gold_created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (
    days(gold_timestamp)
)
"""


CREATE_GOLD_DIM_TIME_SQL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.gold_dim_time (
    time_key STRING,
    time_grain STRING,

    gold_timestamp TIMESTAMP,
    date DATE,

    year INT,
    month INT,
    year_month STRING,
    day INT,
    day_of_week INT,
    hour INT,
    minute INT,

    gold_created_at TIMESTAMP
)
USING iceberg
"""


CREATE_GOLD_DIM_GEOGRAPHY_SQL = f"""
CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.gold_dim_geography (
    geography_key STRING,
    geography_level STRING,
    geography_code STRING,
    geography_name STRING,

    province_code STRING,
    province_name STRING,

    autonomous_community_code STRING,
    autonomous_community_name STRING,

    country_code STRING,
    country_name STRING,

    esios_geo_id BIGINT,

    gold_created_at TIMESTAMP
)
USING iceberg
"""


CREATE_TABLE_STATEMENTS = (
    (
        "gold_fact_province_hourly",
        CREATE_GOLD_FACT_PROVINCE_HOURLY_SQL,
    ),
    (
        "gold_fact_installed_capacity_monthly",
        CREATE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY_SQL,
    ),
    (
        "gold_fact_country_15min",
        CREATE_GOLD_FACT_COUNTRY_15MIN_SQL,
    ),
    (
        "gold_fact_country_5min",
        CREATE_GOLD_FACT_COUNTRY_5MIN_SQL,
    ),
    (
        "gold_dim_time",
        CREATE_GOLD_DIM_TIME_SQL,
    ),
    (
        "gold_dim_geography",
        CREATE_GOLD_DIM_GEOGRAPHY_SQL,
    ),
)


# ============================================================================
# Creation
# ============================================================================

def create_gold_namespace(
    spark: SparkSession,
) -> None:
    """
    Create the approved Gold namespace if it does not already exist.

    Existing namespaces are never dropped or recreated.
    """
    spark.sql(
        CREATE_GOLD_NAMESPACE_SQL
    )


def create_gold_tables(
    spark: SparkSession,
) -> None:
    """
    Create the six approved Gold Iceberg tables.

    CREATE TABLE IF NOT EXISTS is deliberately used so an existing Gold
    table is not dropped, overwritten, or unnecessarily recreated.
    """
    for table_name, ddl in CREATE_TABLE_STATEMENTS:
        print(
            f"CREATE IF NOT EXISTS: "
            f"{GOLD_NAMESPACE}.{table_name}"
        )

        spark.sql(
            ddl
        )


# ============================================================================
# Validation
# ============================================================================

def validate_gold_tables_exist(
    spark: SparkSession,
) -> None:
    """
    Validate that exactly the six approved physical Gold tables exist.
    """
    rows = (
        spark.sql(
            f"SHOW TABLES IN {GOLD_NAMESPACE}"
        )
        .collect()
    )

    existing_tables = {
        row["tableName"]
        for row in rows
    }

    expected_tables = set(
        GOLD_TABLES
    )

    missing_tables = sorted(
        expected_tables
        - existing_tables
    )

    unexpected_tables = sorted(
        existing_tables
        - expected_tables
    )

    print("-" * 80)
    print("GOLD TABLE INVENTORY")
    print(
        f"EXPECTED_TABLES = "
        f"{len(expected_tables)}"
    )
    print(
        f"EXISTING_TABLES = "
        f"{len(existing_tables)}"
    )
    print(
        f"MISSING_TABLES = "
        f"{missing_tables}"
    )
    print(
        f"UNEXPECTED_TABLES = "
        f"{unexpected_tables}"
    )

    for table_name in sorted(
        existing_tables
    ):
        print(
            f"  - {table_name}"
        )

    if missing_tables:
        raise RuntimeError(
            "Missing approved Gold tables: "
            f"{missing_tables}"
        )

    if unexpected_tables:
        raise RuntimeError(
            "Unexpected physical Gold tables found: "
            f"{unexpected_tables}"
        )


def validate_iceberg_provider(
    spark: SparkSession,
) -> None:
    """
    Confirm that every Gold table is physically registered as Iceberg.
    """
    invalid_tables: list[str] = []

    for table_name in GOLD_TABLES:
        full_table_name = (
            f"{GOLD_NAMESPACE}.{table_name}"
        )

        detail_rows = (
            spark.sql(
                f"DESCRIBE TABLE EXTENDED "
                f"{full_table_name}"
            )
            .collect()
        )

        provider = None

        for row in detail_rows:
            column_name = row["col_name"]

            if (
                column_name is not None
                and column_name.strip().lower()
                == "provider"
            ):
                provider = (
                    row["data_type"]
                    .strip()
                    .lower()
                )

                break

        print(
            f"{full_table_name} "
            f"PROVIDER = {provider}"
        )

        if provider != "iceberg":
            invalid_tables.append(
                full_table_name
            )

    if invalid_tables:
        raise RuntimeError(
            "Gold tables not registered as Iceberg: "
            f"{invalid_tables}"
        )


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    spark = get_spark_session(
        "gold-create-physical-tables"
    )

    print("=" * 80)
    print(
        "CREATE PHYSICAL GOLD ICEBERG TABLES"
    )
    print("=" * 80)

    create_gold_namespace(
        spark
    )

    create_gold_tables(
        spark
    )

    validate_gold_tables_exist(
        spark
    )

    validate_iceberg_provider(
        spark
    )

    print("=" * 80)
    print(
        "ALL PHYSICAL GOLD ICEBERG TABLES CREATED AND VALIDATED"
    )
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()