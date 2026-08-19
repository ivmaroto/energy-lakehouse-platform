from pyspark.sql import SparkSession

from silver.common import read_bronze_json


DATASETS = [
    "weather_15min",
    "weather_historical_forecast",
    "weather_hourly",
]


def inspect_dataset(
    spark: SparkSession,
    dataset: str,
) -> None:
    df = read_bronze_json(
        spark=spark,
        source="open_meteo",
        dataset=dataset,
        multiline=True,
    )

    print("=" * 80)
    print(f"OPEN-METEO DATASET = {dataset}")
    print("=" * 80)

    print("TOP LEVEL COLUMNS =", sorted(df.columns))

    print("=" * 80)
    print("SPARK SCHEMA")
    print("=" * 80)

    df.printSchema()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("inspect-open-meteo-bronze")
        .getOrCreate()
    )

    for dataset in DATASETS:
        inspect_dataset(
            spark,
            dataset,
        )

    spark.stop()


if __name__ == "__main__":
    main()