from pyspark.sql import SparkSession

from silver.aemet import read_current_observations_bronze


def main():
    spark = (
        SparkSession.builder
        .appName("inspect-aemet-current-schema")
        .getOrCreate()
    )

    df = read_current_observations_bronze(spark)

    print("=" * 80)
    print("AEMET CURRENT OBSERVATIONS REAL SCHEMA")
    print("=" * 80)

    print("COLUMNS =", sorted(df.columns))

    print("=" * 80)
    print("SPARK SCHEMA")
    print("=" * 80)

    df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()