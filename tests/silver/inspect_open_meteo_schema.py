from pyspark.sql import SparkSession

from silver.open_meteo import read_weather_bronze


def main():
    spark = (
        SparkSession.builder
        .appName("inspect-open-meteo-schema")
        .getOrCreate()
    )

    df = read_weather_bronze(spark)

    print("=" * 80)
    print("OPEN-METEO BRONZE REAL SCHEMA")
    print("=" * 80)

    print("COLUMNS =", sorted(df.columns))

    print("=" * 80)
    print("SPARK SCHEMA")
    print("=" * 80)

    df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
