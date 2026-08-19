from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.esios import (
    ESIOS_DATASETS,
    MAGNITUDE_ENERGY_ID,
    MAGNITUDE_POWER_ID,
    TIME_FIVE_MINUTES_ID,
    TIME_HOUR_ID,
    TIME_MONTH_ID,
    build_all_esios_observations,
    build_esios_energy_hourly,
    build_esios_installed_capacity_monthly,
    build_esios_power_5min,
    read_esios_dataset_bronze,
    transform_esios_dataset,
)


EMPTY_VALUE_DATASETS = [
    "demanda_en_consumo",
    "demanda_medida_discriminacion_horaria_total",
]


def validate_family(
    name,
    df,
    expected_magnitude_id,
    expected_time_id,
    expected_minutes=None,
):
    print("=" * 80)
    print(f"FAMILY = {name}")
    print("=" * 80)

    rows = df.count()

    distinct_keys = (
        df
        .select(
            "indicator_id",
            "esios_geo_id",
            "observation_timestamp",
        )
        .distinct()
        .count()
    )

    null_indicator_id = (
        df
        .filter(F.col("indicator_id").isNull())
        .count()
    )

    null_geo_id = (
        df
        .filter(F.col("esios_geo_id").isNull())
        .count()
    )

    null_timestamp = (
        df
        .filter(
            F.col("observation_timestamp").isNull()
        )
        .count()
    )

    null_value = (
        df
        .filter(F.col("value").isNull())
        .count()
    )

    null_source = (
        df
        .filter(F.col("source").isNull())
        .count()
    )

    null_ingestion_timestamp = (
        df
        .filter(
            F.col("ingestion_timestamp").isNull()
        )
        .count()
    )

    duplicate_keys = (
        rows - distinct_keys
    )

    wrong_magnitude = (
        df
        .filter(
            F.col("magnitude_id")
            != expected_magnitude_id
        )
        .count()
    )

    wrong_time = (
        df
        .filter(
            F.col("time_id")
            != expected_time_id
        )
        .count()
    )

    dataset_count = (
        df
        .select("dataset")
        .distinct()
        .count()
    )

    indicator_count = (
        df
        .select("indicator_id")
        .distinct()
        .count()
    )

    geography_count = (
        df
        .select(
            "esios_geo_id",
            "esios_geo_name",
        )
        .distinct()
        .count()
    )

    print("ROWS =", rows)
    print("DISTINCT_KEYS =", distinct_keys)
    print("DATASETS_WITH_ROWS =", dataset_count)
    print("DISTINCT_INDICATORS =", indicator_count)
    print("DISTINCT_GEOGRAPHIES =", geography_count)

    print(
        "NULL_INDICATOR_ID =",
        null_indicator_id,
    )

    print(
        "NULL_ESIOS_GEO_ID =",
        null_geo_id,
    )

    print(
        "NULL_OBSERVATION_TIMESTAMP =",
        null_timestamp,
    )

    print(
        "NULL_VALUE =",
        null_value,
    )

    print(
        "NULL_SOURCE =",
        null_source,
    )

    print(
        "NULL_INGESTION_TIMESTAMP =",
        null_ingestion_timestamp,
    )

    print(
        "DUPLICATE_KEYS =",
        duplicate_keys,
    )

    print(
        "WRONG_MAGNITUDE_ID =",
        wrong_magnitude,
    )

    print(
        "WRONG_TIME_ID =",
        wrong_time,
    )

    if expected_minutes is not None:
        window = (
            Window
            .partitionBy(
                "indicator_id",
                "esios_geo_id",
            )
            .orderBy(
                "observation_timestamp"
            )
        )

        temporal = (
            df
            .select(
                "indicator_id",
                "esios_geo_id",
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
                ).over(window),
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

        total_differences = (
            temporal
            .filter(
                F.col(
                    "previous_timestamp"
                ).isNotNull()
            )
            .count()
        )

        matching_granularity = (
            temporal
            .filter(
                F.col(
                    "previous_timestamp"
                ).isNotNull()
            )
            .filter(
                F.col(
                    "diff_minutes"
                ) == expected_minutes
            )
            .count()
        )

        print(
            "EXPECTED_GRANULARITY_MINUTES =",
            expected_minutes,
        )

        print(
            "TOTAL_TEMPORAL_DIFFERENCES =",
            total_differences,
        )

        print(
            "MATCHING_GRANULARITY =",
            matching_granularity,
        )


def validate_empty_source_datasets(
    spark,
):
    print("=" * 80)
    print("EMPTY VALUES DATASETS")
    print("=" * 80)

    for dataset in EMPTY_VALUE_DATASETS:
        bronze = read_esios_dataset_bronze(
            spark,
            dataset,
        )

        transformed = transform_esios_dataset(
            bronze,
            dataset,
        )

        print(
            f"EMPTY_DATASET_ROWS {dataset} =",
            transformed.count(),
        )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName(
            "silver-esios-integration-test"
        )
        .getOrCreate()
    )

    print("=" * 80)
    print("ESIOS SILVER INTEGRATION TEST")
    print("=" * 80)

    print(
        "CONFIGURED_DATASETS =",
        len(ESIOS_DATASETS),
    )

    observations = (
        build_all_esios_observations(
            spark
        )
    )

    energy_hourly = (
        build_esios_energy_hourly(
            observations
        )
    )

    power_5min = (
        build_esios_power_5min(
            observations
        )
    )

    installed_capacity_monthly = (
        build_esios_installed_capacity_monthly(
            observations
        )
    )

    validate_family(
        name="silver_esios_energy_hourly",
        df=energy_hourly,
        expected_magnitude_id=MAGNITUDE_ENERGY_ID,
        expected_time_id=TIME_HOUR_ID,
        expected_minutes=60,
    )

    validate_family(
        name="silver_esios_power_5min",
        df=power_5min,
        expected_magnitude_id=MAGNITUDE_POWER_ID,
        expected_time_id=TIME_FIVE_MINUTES_ID,
        expected_minutes=5,
    )

    validate_family(
        name="silver_esios_installed_capacity_monthly",
        df=installed_capacity_monthly,
        expected_magnitude_id=MAGNITUDE_POWER_ID,
        expected_time_id=TIME_MONTH_ID,
        expected_minutes=None,
    )

    print("=" * 80)
    print("CLASSIFICATION")
    print("=" * 80)

    classified_datasets = (
        observations
        .select(
            "dataset",
            "magnitude_id",
            "time_id",
        )
        .distinct()
    )

    print(
        "ENERGY_HOURLY_DATASETS =",
        classified_datasets
        .filter(
            (F.col("magnitude_id") == MAGNITUDE_ENERGY_ID)
            &
            (F.col("time_id") == TIME_HOUR_ID)
        )
        .select("dataset")
        .distinct()
        .count(),
    )

    print(
        "POWER_5MIN_DATASETS =",
        classified_datasets
        .filter(
            (F.col("magnitude_id") == MAGNITUDE_POWER_ID)
            &
            (
                F.col("time_id")
                == TIME_FIVE_MINUTES_ID
            )
        )
        .select("dataset")
        .distinct()
        .count(),
    )

    print(
        "INSTALLED_CAPACITY_MONTHLY_DATASETS =",
        classified_datasets
        .filter(
            (F.col("magnitude_id") == MAGNITUDE_POWER_ID)
            &
            (F.col("time_id") == TIME_MONTH_ID)
        )
        .select("dataset")
        .distinct()
        .count(),
    )

    validate_empty_source_datasets(
        spark
    )

    spark.stop()


if __name__ == "__main__":
    main()