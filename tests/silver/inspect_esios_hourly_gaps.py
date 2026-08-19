from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.esios import (
    build_all_esios_observations,
    build_esios_energy_hourly,
)


def main():
    spark = (
        SparkSession.builder
        .appName("inspect-esios-hourly-gaps")
        .getOrCreate()
    )

    observations = build_all_esios_observations(spark)
    hourly = build_esios_energy_hourly(observations)

    window = (
        Window
        .partitionBy(
            "dataset",
            "indicator_id",
            "esios_geo_id",
        )
        .orderBy(
            "observation_timestamp"
        )
    )

    differences = (
        hourly
        .withColumn(
            "previous_timestamp",
            F.lag(
                "observation_timestamp"
            ).over(window),
        )
        .withColumn(
            "diff_minutes",
            (
                F.col("observation_timestamp").cast("long")
                - F.col("previous_timestamp").cast("long")
            ) / 60,
        )
        .filter(
            F.col("previous_timestamp").isNotNull()
        )
    )

    print("=" * 80)
    print("ESIOS HOURLY DIFFERENCE DISTRIBUTION")
    print("=" * 80)

    (
        differences
        .groupBy("diff_minutes")
        .count()
        .orderBy("diff_minutes")
        .show(100, truncate=False)
    )

    print("=" * 80)
    print("NON-60-MINUTE DIFFERENCES BY DATASET")
    print("=" * 80)

    (
        differences
        .filter(F.col("diff_minutes") != 60)
        .groupBy(
            "dataset",
            "diff_minutes",
        )
        .count()
        .orderBy(
            "dataset",
            "diff_minutes",
        )
        .show(200, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()