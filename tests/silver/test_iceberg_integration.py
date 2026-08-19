from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("iceberg-integration-test")
        .getOrCreate()
    )

    namespace = "lakehouse.silver_test"
    table = f"{namespace}.integration_test"

    print("=" * 80)
    print("ICEBERG INTEGRATION TEST")
    print("=" * 80)

    # Clean previous test execution if necessary.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    # Small controlled dataset.
    df = spark.createDataFrame(
        [
            (1, "silver-test"),
            (2, "iceberg-test"),
        ],
        ["id", "description"],
    )

    # Create a real Iceberg table through the configured catalog.
    (
        df.writeTo(table)
        .using("iceberg")
        .create()
    )

    result = spark.table(table)

    print(f"ROWS = {result.count()}")
    result.printSchema()
    result.orderBy("id").show(truncate=False)

    # Remove the test table after successful validation.
    spark.sql(f"DROP TABLE {table}")
    spark.sql(f"DROP NAMESPACE {namespace}")

    spark.stop()


if __name__ == "__main__":
    main()