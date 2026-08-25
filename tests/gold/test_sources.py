from gold.common import (
    GOLD_SOURCE_TABLES,
    get_spark_session,
    read_silver_table,
    validate_gold_source_tables,
)


def main() -> None:
    spark = get_spark_session(
        "gold-validate-silver-sources"
    )

    print("=" * 80)
    print("VALIDATE GOLD SILVER SOURCES")
    print("=" * 80)

    validate_gold_source_tables(
        spark
    )

    for table_name in GOLD_SOURCE_TABLES:
        df = read_silver_table(
            spark=spark,
            table_name=table_name,
        )

        print(
            f"{table_name} = {df.count()} rows"
        )

    print("=" * 80)
    print("ALL REQUIRED SILVER SOURCES ARE ACCESSIBLE")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()