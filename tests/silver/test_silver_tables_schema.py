from pyspark.sql import SparkSession


TABLES = [
    "lakehouse.silver.silver_aemet_stations",
    "lakehouse.silver.silver_aemet_daily_climatology",
    "lakehouse.silver.silver_aemet_current_observations",
    "lakehouse.silver.silver_open_meteo_hourly",
    "lakehouse.silver.silver_open_meteo_historical_forecast",
    "lakehouse.silver.silver_open_meteo_15min",
    "lakehouse.silver.silver_cnig_provinces",
    "lakehouse.silver.silver_cnig_autonomous_communities",
    "lakehouse.silver.silver_cnig_municipalities",
    "lakehouse.silver.silver_esios_energy_hourly",
    "lakehouse.silver.silver_esios_power_5min",
    "lakehouse.silver.silver_esios_installed_capacity_monthly",
]


EXPECTED_PARTITIONING = {
    "lakehouse.silver.silver_aemet_stations": None,
    "lakehouse.silver.silver_aemet_daily_climatology":
        "months(observation_date)",
    "lakehouse.silver.silver_aemet_current_observations":
        "days(observation_timestamp)",

    "lakehouse.silver.silver_open_meteo_hourly":
        "days(observation_timestamp)",
    "lakehouse.silver.silver_open_meteo_historical_forecast":
        "days(observation_timestamp)",
    "lakehouse.silver.silver_open_meteo_15min":
        "days(observation_timestamp)",

    "lakehouse.silver.silver_cnig_provinces": None,
    "lakehouse.silver.silver_cnig_autonomous_communities": None,
    "lakehouse.silver.silver_cnig_municipalities": None,

    "lakehouse.silver.silver_esios_energy_hourly":
        "days(observation_timestamp)",
    "lakehouse.silver.silver_esios_power_5min":
        "days(observation_timestamp)",
    "lakehouse.silver.silver_esios_installed_capacity_monthly":
        "months(observation_timestamp)",
}


EXPECTED_CANONICAL_GEOGRAPHY_COLUMNS = {
    "lakehouse.silver.silver_aemet_stations": {
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
    },
    "lakehouse.silver.silver_aemet_daily_climatology": {
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
    },
    "lakehouse.silver.silver_open_meteo_hourly": {
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
    },
    "lakehouse.silver.silver_open_meteo_historical_forecast": {
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
    },
    "lakehouse.silver.silver_open_meteo_15min": {
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
    },
}


def main():
    spark = (
        SparkSession.builder
        .appName("validate-silver-table-schemas")
        .getOrCreate()
    )

    print("=" * 80)
    print("SILVER TABLE PHYSICAL VALIDATION")
    print("=" * 80)

    found_tables = (
        spark.sql(
            "SHOW TABLES IN lakehouse.silver"
        )
        .select("tableName")
        .collect()
    )

    found_names = {
        row["tableName"]
        for row in found_tables
    }

    print("TABLE_COUNT =", len(found_names))

    missing = []

    for full_name in TABLES:
        short_name = full_name.split(".")[-1]

        if short_name not in found_names:
            missing.append(full_name)

    print("MISSING_TABLES =", missing)

    for table_name in TABLES:
        print("=" * 80)
        print(f"TABLE = {table_name}")
        print("=" * 80)

        df = spark.table(table_name)

        print("COLUMNS =", len(df.columns))
        print("SCHEMA =", df.schema.simpleString())

        expected_columns = (
            EXPECTED_CANONICAL_GEOGRAPHY_COLUMNS.get(
                table_name,
                set(),
            )
        )

        missing_expected_columns = sorted(
            expected_columns
            - set(df.columns)
        )

        print(
            "EXPECTED_CANONICAL_GEOGRAPHY_COLUMNS =",
            sorted(expected_columns),
        )

        print(
            "MISSING_CANONICAL_GEOGRAPHY_COLUMNS =",
            missing_expected_columns,
        )

        print(
            "CANONICAL_GEOGRAPHY_SCHEMA_OK =",
            len(missing_expected_columns) == 0,
        )

        description = (
            spark.sql(
                f"DESCRIBE TABLE EXTENDED {table_name}"
            )
            .collect()
        )

        partition_lines = []

        in_partition_section = False

        for row in description:
            col_name = row["col_name"]
            data_type = row["data_type"]

            if col_name == "# Partitioning":
                in_partition_section = True
                continue

            if in_partition_section:
                if (
                    col_name is None
                    or col_name == ""
                    or col_name.startswith("#")
                ):
                    break

                partition_lines.append(
                    data_type
                )

        expected_partitioning = (
            EXPECTED_PARTITIONING[
                table_name
            ]
        )

        if expected_partitioning is None:
            partition_ok = (
                len(partition_lines) == 0
            )
        else:
            partition_ok = (
                expected_partitioning
                in partition_lines
            )

        print(
            "PARTITIONING =",
            partition_lines,
        )

        print(
            "EXPECTED_PARTITIONING =",
            expected_partitioning,
        )

        print(
            "PARTITION_OK =",
            partition_ok,
        )

        provider = None
        location = None

        for row in description:
            if row["col_name"] == "Provider":
                provider = row["data_type"]

            if row["col_name"] == "Location":
                location = row["data_type"]

        print(
            "PROVIDER =",
            provider,
        )

        print(
            "LOCATION =",
            location,
        )

        print(
            "PROVIDER_OK =",
            provider == "iceberg",
        )

        print(
            "LOCATION_OK =",
            location is not None
            and location.startswith(
                "s3://energy-lakehouse/warehouse/silver/"
            ),
        )

    spark.stop()


if __name__ == "__main__":
    main()