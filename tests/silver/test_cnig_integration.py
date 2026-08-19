from pyspark.sql import SparkSession

from silver.cnig import build_cnig_silver


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("silver-cnig-integration-test")
        .getOrCreate()
    )

    (
        provinces,
        autonomous_communities,
        municipalities,
    ) = build_cnig_silver(spark)

    print("=" * 80)
    print("CNIG SILVER INTEGRATION TEST")
    print("=" * 80)

    print("PROVINCES =", provinces.count())
    print(
        "AUTONOMOUS_COMMUNITIES =",
        autonomous_communities.count(),
    )
    print("MUNICIPALITIES =", municipalities.count())

    print(
        "MUNICIPALITY_INE_DISTINCT =",
        municipalities
        .select("municipality_ine_code")
        .distinct()
        .count(),
    )

    print(
        "MUNICIPALITY_CODE_00000 =",
        municipalities
        .filter("municipality_code = '00000'")
        .count(),
    )

    print(
        "NULL_MUNICIPALITY_INE_CODE =",
        municipalities
        .filter("municipality_ine_code IS NULL")
        .count(),
    )

    print(
        "NULL_PROVINCE_CODE =",
        provinces
        .filter("province_code IS NULL")
        .count(),
    )

    spark.stop()


if __name__ == "__main__":
    main()