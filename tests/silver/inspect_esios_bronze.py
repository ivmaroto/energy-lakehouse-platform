from silver.esios import ESIOS_DATASETS
from pyspark.sql import SparkSession

from silver.common import read_bronze_json





def main():
    spark = (
        SparkSession.builder
        .appName("inspect-esios-bronze")
        .getOrCreate()
    )

    print("=" * 80)
    print("ESIOS BRONZE INSPECTION")
    print("=" * 80)

    for dataset in ESIOS_DATASETS:
        print("=" * 80)
        print(f"DATASET = {dataset}")
        print("=" * 80)

        df = read_bronze_json(
            spark=spark,
            source="esios",
            dataset=dataset,
            multiline=True,
        )

        print("TOP LEVEL COLUMNS =", sorted(df.columns))
        df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()