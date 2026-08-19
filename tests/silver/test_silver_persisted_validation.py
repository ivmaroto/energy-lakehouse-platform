from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


TABLES = {
    "silver_aemet_stations": {
        "expected_rows": 921,
        "key": ["station_id"],
    },
    "silver_aemet_daily_climatology": {
        "expected_rows": 2420,
        "key": ["station_id", "observation_date"],
    },
    "silver_aemet_current_observations": {
        "expected_rows": 9688,
        "key": ["station_id", "observation_timestamp"],
    },
    "silver_open_meteo_hourly": {
        "expected_rows": 88416,
        "key": ["station_id", "observation_timestamp"],
        "expected_minutes": 60,
    },
    "silver_open_meteo_historical_forecast": {
        "expected_rows": 88416,
        "key": ["station_id", "observation_timestamp"],
        "expected_minutes": 60,
    },
    "silver_open_meteo_15min": {
        "expected_rows": 353664,
        "key": ["station_id", "observation_timestamp"],
        "expected_minutes": 15,
    },
    "silver_cnig_provinces": {
        "expected_rows": 52,
        "key": ["province_code"],
    },
    "silver_cnig_autonomous_communities": {
        "expected_rows": 19,
        "key": ["autonomous_community_code"],
    },
    "silver_cnig_municipalities": {
        "expected_rows": 8132,
        "key": ["municipality_ine_code"],
    },
    "silver_esios_energy_hourly": {
        "expected_rows": 30107,
        "key": [
            "indicator_id",
            "esios_geo_id",
            "observation_timestamp",
        ],
        "expected_minutes": 60,
        "allow_temporal_gaps": True,
    },
    "silver_esios_power_5min": {
        "expected_rows": 13824,
        "key": [
            "indicator_id",
            "esios_geo_id",
            "observation_timestamp",
        ],
        "expected_minutes": 5,
    },
    "silver_esios_installed_capacity_monthly": {
        "expected_rows": 123,
        "key": [
            "indicator_id",
            "esios_geo_id",
            "observation_timestamp",
        ],
    },
}


def count_null_keys(df, key):
    condition = None

    for column in key:
        current = F.col(column).isNull()
        condition = current if condition is None else condition | current

    return df.filter(condition).count()


def count_duplicates(df, key):
    return (
        df
        .groupBy(*key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def validate_temporal_granularity(
    df,
    expected_minutes,
    partition_columns,
):
    window = (
        Window
        .partitionBy(*partition_columns)
        .orderBy("observation_timestamp")
    )

    diffs = (
        df
        .select(
            *partition_columns,
            "observation_timestamp",
        )
        .withColumn(
            "previous_timestamp",
            F.lag("observation_timestamp").over(window),
        )
        .withColumn(
            "diff_minutes",
            (
                F.col("observation_timestamp").cast("long")
                - F.col("previous_timestamp").cast("long")
            ) / 60,
        )
        .filter(F.col("previous_timestamp").isNotNull())
    )

    total = diffs.count()

    matching = (
        diffs
        .filter(F.col("diff_minutes") == expected_minutes)
        .count()
    )

    return total, matching


def main():
    spark = (
        SparkSession.builder
        .appName("silver-persisted-validation")
        .getOrCreate()
    )

    print("=" * 80)
    print("SILVER PERSISTED DATA VALIDATION")
    print("=" * 80)

    for table, config in TABLES.items():
        full_name = f"lakehouse.silver.{table}"

        print("=" * 80)
        print(f"TABLE = {table}")

        df = spark.table(full_name)

        rows = df.count()
        null_keys = count_null_keys(
            df,
            config["key"],
        )
        duplicates = count_duplicates(
            df,
            config["key"],
        )

        print("ROWS =", rows)
        print("EXPECTED_ROWS =", config["expected_rows"])
        print("ROW_COUNT_OK =", rows == config["expected_rows"])

        print("NULL_NATURAL_KEYS =", null_keys)
        print("DUPLICATE_KEYS =", duplicates)

        timestamp_columns = [
            column
            for column in [
                "observation_timestamp",
                "observation_date",
                "ingestion_timestamp",
            ]
            if column in df.columns
        ]

        for column in timestamp_columns:
            null_count = (
                df
                .filter(F.col(column).isNull())
                .count()
            )

            print(
                f"NULL_{column.upper()} =",
                null_count,
            )

        if (
            "latitude" in df.columns
            and "longitude" in df.columns
        ):
            invalid_coordinates = (
                df
                .filter(
                    (F.col("latitude") < -90)
                    | (F.col("latitude") > 90)
                    | (F.col("longitude") < -180)
                    | (F.col("longitude") > 180)
                )
                .count()
            )

            print(
                "INVALID_COORDINATES =",
                invalid_coordinates,
            )

        if "expected_minutes" in config:
            if table.startswith("silver_esios"):
                partition_columns = [
                    "indicator_id",
                    "esios_geo_id",
                ]
            else:
                partition_columns = [
                    "station_id",
                ]

            total, matching = validate_temporal_granularity(
                df,
                config["expected_minutes"],
                partition_columns,
            )

            print(
                "EXPECTED_GRANULARITY_MINUTES =",
                config["expected_minutes"],
            )
            print(
                "TOTAL_TEMPORAL_DIFFERENCES =",
                total,
            )
            print(
                "MATCHING_GRANULARITY =",
                matching,
            )
            print(
                "TEMPORAL_GAPS_OR_DIFFERENCES =",
                total - matching,
            )

    print("=" * 80)
    print("SILVER PERSISTED DATA VALIDATION COMPLETE")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()