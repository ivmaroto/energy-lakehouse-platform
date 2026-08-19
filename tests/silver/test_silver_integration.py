from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("silver-integration-test")
        .getOrCreate()
    )

    bronze_path = (
        "s3a://energy-lakehouse/"
        "bronze/aemet/stations/"
    )

    print("=" * 80)
    print("SILVER INTEGRATION TEST")
    print("=" * 80)
    print(f"Reading Bronze path: {bronze_path}")

    df = (
        spark.read
        .option("multiLine", "true")
        .json(bronze_path)
    )

    print(f"WRAPPER_ROWS = {df.count()}")
    df.printSchema()

    df.select(
        "metadata.source",
        "metadata.dataset",
    ).show(
        5,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()