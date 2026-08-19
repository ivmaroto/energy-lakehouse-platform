from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.aemet import build_aemet_silver


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("silver-aemet-integration-test")
        .getOrCreate()
    )

    (
        stations,
        daily,
        current,
    ) = build_aemet_silver(spark)

    print("=" * 80)
    print("AEMET SILVER INTEGRATION TEST")
    print("=" * 80)

    print("STATIONS =", stations.count())
    print("DAILY =", daily.count())
    print("CURRENT =", current.count())

    print(
        "STATION_ID_DISTINCT =",
        stations
        .select("station_id")
        .distinct()
        .count(),
    )

    print(
        "NULL_STATION_ID_STATIONS =",
        stations
        .filter(F.col("station_id").isNull())
        .count(),
    )

    print(
        "NULL_STATION_ID_DAILY =",
        daily
        .filter(F.col("station_id").isNull())
        .count(),
    )

    print(
        "NULL_OBSERVATION_DATE =",
        daily
        .filter(F.col("observation_date").isNull())
        .count(),
    )

    print(
        "NULL_STATION_ID_CURRENT =",
        current
        .filter(F.col("station_id").isNull())
        .count(),
    )

    print(
        "NULL_OBSERVATION_TIMESTAMP =",
        current
        .filter(F.col("observation_timestamp").isNull())
        .count(),
    )

    print(
        "DUPLICATE_STATION_KEYS =",
        stations.count()
        - stations
        .select("station_id")
        .distinct()
        .count(),
    )

    print(
        "DUPLICATE_DAILY_KEYS =",
        daily.count()
        - daily
        .select(
            "station_id",
            "observation_date",
        )
        .distinct()
        .count(),
    )

    print(
        "DUPLICATE_CURRENT_KEYS =",
        current.count()
        - current
        .select(
            "station_id",
            "observation_timestamp",
        )
        .distinct()
        .count(),
    )

    print(
        "INVALID_STATION_COORDINATES =",
        stations
        .filter(
            (F.col("latitude").isNotNull())
            & (
                (F.col("latitude") < -90)
                | (F.col("latitude") > 90)
            )
            |
            (F.col("longitude").isNotNull())
            & (
                (F.col("longitude") < -180)
                | (F.col("longitude") > 180)
            )
        )
        .count(),
    )

    print(
        "INVALID_CURRENT_COORDINATES =",
        current
        .filter(
            (F.col("latitude").isNotNull())
            & (
                (F.col("latitude") < -90)
                | (F.col("latitude") > 90)
            )
            |
            (F.col("longitude").isNotNull())
            & (
                (F.col("longitude") < -180)
                | (F.col("longitude") > 180)
            )
        )
        .count(),
    )

    spark.stop()


if __name__ == "__main__":
    main()