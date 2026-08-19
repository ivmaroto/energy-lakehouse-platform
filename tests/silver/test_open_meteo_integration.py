from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.open_meteo import build_open_meteo_silver


def validate_dataset(
    name,
    df,
    expected_minutes,
):
    print("=" * 80)
    print(f"DATASET = {name}")
    print("=" * 80)

    count = df.count()

    distinct_keys = (
        df
        .select(
            "station_id",
            "observation_timestamp",
        )
        .distinct()
        .count()
    )

    null_station_id = (
        df
        .filter(
            F.col("station_id").isNull()
        )
        .count()
    )

    null_timestamp = (
        df
        .filter(
            F.col("observation_timestamp").isNull()
        )
        .count()
    )

    duplicate_keys = (
        count
        - distinct_keys
    )

    invalid_coordinates = (
        df
        .filter(
            (
                F.col("latitude").isNotNull()
                & (
                    (F.col("latitude") < -90)
                    | (F.col("latitude") > 90)
                )
            )
            |
            (
                F.col("longitude").isNotNull()
                & (
                    (F.col("longitude") < -180)
                    | (F.col("longitude") > 180)
                )
            )
        )
        .count()
    )

    null_source = (
        df
        .filter(
            F.col("source").isNull()
        )
        .count()
    )

    null_ingestion_timestamp = (
        df
        .filter(
            F.col("ingestion_timestamp").isNull()
        )
        .count()
    )

    print("ROWS =", count)
    print("DISTINCT_KEYS =", distinct_keys)

    print(
        "NULL_STATION_ID =",
        null_station_id,
    )

    print(
        "NULL_OBSERVATION_TIMESTAMP =",
        null_timestamp,
    )

    print(
        "DUPLICATE_KEYS =",
        duplicate_keys,
    )

    print(
        "INVALID_COORDINATES =",
        invalid_coordinates,
    )

    print(
        "NULL_SOURCE =",
        null_source,
    )

    print(
        "NULL_INGESTION_TIMESTAMP =",
        null_ingestion_timestamp,
    )

    # ----------------------------------------------------------------------
    # Temporal granularity validation
    # ----------------------------------------------------------------------

    station_windows = (
        df
        .select(
            "station_id",
            "observation_timestamp",
        )
        .where(
            F.col(
                "observation_timestamp"
            ).isNotNull()
        )
        .withColumn(
            "previous_timestamp",
            F.lag(
                "observation_timestamp"
            ).over(
                __import__(
                    "pyspark.sql.window",
                    fromlist=["Window"],
                )
                .Window
                .partitionBy(
                    "station_id"
                )
                .orderBy(
                    "observation_timestamp"
                )
            ),
        )
        .withColumn(
            "diff_minutes",
            (
                F.col(
                    "observation_timestamp"
                ).cast("long")
                -
                F.col(
                    "previous_timestamp"
                ).cast("long")
            )
            / 60
        )
    )

    matching_granularity = (
        station_windows
        .filter(
            F.col(
                "previous_timestamp"
            ).isNotNull()
        )
        .filter(
            F.col(
                "diff_minutes"
            )
            == expected_minutes
        )
        .count()
    )

    total_diffs = (
        station_windows
        .filter(
            F.col(
                "previous_timestamp"
            ).isNotNull()
        )
        .count()
    )

    print(
        "EXPECTED_GRANULARITY_MINUTES =",
        expected_minutes,
    )

    print(
        "TOTAL_TEMPORAL_DIFFERENCES =",
        total_diffs,
    )

    print(
        "MATCHING_GRANULARITY =",
        matching_granularity,
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName(
            "silver-open-meteo-integration-test"
        )
        .getOrCreate()
    )

    (
        hourly,
        historical_forecast,
        weather_15min,
    ) = build_open_meteo_silver(
        spark
    )

    print("=" * 80)
    print(
        "OPEN-METEO SILVER INTEGRATION TEST"
    )
    print("=" * 80)

    validate_dataset(
        name="weather_hourly",
        df=hourly,
        expected_minutes=60,
    )

    validate_dataset(
        name="weather_historical_forecast",
        df=historical_forecast,
        expected_minutes=60,
    )

    validate_dataset(
        name="weather_15min",
        df=weather_15min,
        expected_minutes=15,
    )

    spark.stop()


if __name__ == "__main__":
    main()