from __future__ import annotations

from pyspark.sql import functions as F

from gold.common import (
    TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES,
    TABLE_SILVER_CNIG_PROVINCES,
    TABLE_SILVER_ESIOS_ENERGY_HOURLY,
    TABLE_SILVER_ESIOS_INSTALLED_CAPACITY_MONTHLY,
    get_spark_session,
    read_silver_table,
)

from gold.geography import (
    normalize_gold_autonomous_communities,
    normalize_gold_provinces,
)


def count_unmatched(
    df,
    required_columns: list[str],
) -> int:
    condition = None

    for column_name in required_columns:
        current = F.col(column_name).isNull()

        condition = (
            current
            if condition is None
            else condition | current
        )

    return (
        df
        .filter(condition)
        .count()
    )


def count_duplicate_keys(
    df,
    key_columns: list[str],
) -> int:
    return (
        df
        .groupBy(
            *key_columns
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )


def main() -> None:
    spark = get_spark_session(
        "gold-validate-geographical-normalization"
    )

    print("=" * 80)
    print("VALIDATE GOLD GEOGRAPHICAL NORMALIZATION")
    print("=" * 80)

    # ========================================================================
    # Read canonical CNIG masters
    # ========================================================================

    cnig_provinces = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_CNIG_PROVINCES,
    )

    cnig_autonomous_communities = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES,
    )

    # ========================================================================
    # Province normalization - ESIOS hourly
    # ========================================================================

    esios_hourly = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_ESIOS_ENERGY_HOURLY,
    )

    esios_hourly_province_rows = (
        esios_hourly
        .filter(
            F.col("esios_geo_name").isNotNull()
        )
    )

    normalized_provinces = (
        normalize_gold_provinces(
            esios_hourly_province_rows,
            cnig_provinces,
            source_province_column="esios_geo_name",
        )
    )

    province_rows = (
        normalized_provinces
        .count()
    )

    province_unmatched = count_unmatched(
        normalized_provinces,
        [
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    province_distinct_sources = (
        normalized_provinces
        .select(
            "esios_geo_name"
        )
        .distinct()
        .count()
    )

    province_distinct_canonical = (
        normalized_provinces
        .select(
            "province_code",
            "province_name",
        )
        .distinct()
        .count()
    )

    print("-" * 80)
    print("ESIOS_HOURLY_PROVINCE_NORMALIZATION")
    print(
        f"ROWS = {province_rows}"
    )
    print(
        f"DISTINCT_SOURCE_NAMES = "
        f"{province_distinct_sources}"
    )
    print(
        f"DISTINCT_CANONICAL_PROVINCES = "
        f"{province_distinct_canonical}"
    )
    print(
        f"UNMATCHED_ROWS = {province_unmatched}"
    )

    if province_unmatched != 0:
        print(
            "UNMATCHED PROVINCE SOURCE NAMES:"
        )

        (
            normalized_provinces
            .filter(
                F.col("province_code").isNull()
                |
                F.col("province_name").isNull()
            )
            .select(
                "esios_geo_name"
            )
            .distinct()
            .orderBy(
                "esios_geo_name"
            )
            .show(
                100,
                truncate=False,
            )
        )

        raise RuntimeError(
            "Province normalization contains "
            "unmatched ESIOS geographical names."
        )

    # ========================================================================
    # CCAA normalization - installed capacity monthly
    # ========================================================================

    installed_capacity = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_ESIOS_INSTALLED_CAPACITY_MONTHLY,
    )

    normalized_communities = (
        normalize_gold_autonomous_communities(
            installed_capacity,
            cnig_autonomous_communities,
            source_autonomous_community_column="esios_geo_name",
        )
    )

    community_rows = (
        normalized_communities
        .count()
    )

    community_unmatched = count_unmatched(
        normalized_communities,
        [
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    community_distinct_sources = (
        normalized_communities
        .select(
            "esios_geo_name"
        )
        .distinct()
        .count()
    )

    community_distinct_canonical = (
        normalized_communities
        .select(
            "autonomous_community_code",
            "autonomous_community_name",
        )
        .distinct()
        .count()
    )

    print("-" * 80)
    print("ESIOS_MONTHLY_CCAA_NORMALIZATION")
    print(
        f"ROWS = {community_rows}"
    )
    print(
        f"DISTINCT_SOURCE_NAMES = "
        f"{community_distinct_sources}"
    )
    print(
        f"DISTINCT_CANONICAL_CCAA = "
        f"{community_distinct_canonical}"
    )
    print(
        f"UNMATCHED_ROWS = {community_unmatched}"
    )

    if community_unmatched != 0:
        print(
            "UNMATCHED CCAA SOURCE NAMES:"
        )

        (
            normalized_communities
            .filter(
                F.col(
                    "autonomous_community_code"
                ).isNull()
                |
                F.col(
                    "autonomous_community_name"
                ).isNull()
            )
            .select(
                "esios_geo_name"
            )
            .distinct()
            .orderBy(
                "esios_geo_name"
            )
            .show(
                100,
                truncate=False,
            )
        )

        raise RuntimeError(
            "Autonomous-community normalization contains "
            "unmatched ESIOS geographical names."
        )

    # ========================================================================
    # Duplicate canonical mappings
    # ========================================================================

    province_mapping_duplicates = (
        normalized_provinces
        .select(
            "esios_geo_name",
            "province_code",
        )
        .distinct()
        .groupBy(
            "esios_geo_name"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    community_mapping_duplicates = (
        normalized_communities
        .select(
            "esios_geo_name",
            "autonomous_community_code",
        )
        .distinct()
        .groupBy(
            "esios_geo_name"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print("-" * 80)
    print("CANONICAL_MAPPING_VALIDATION")
    print(
        f"PROVINCE_SOURCE_NAMES_WITH_MULTIPLE_MATCHES = "
        f"{province_mapping_duplicates}"
    )
    print(
        f"CCAA_SOURCE_NAMES_WITH_MULTIPLE_MATCHES = "
        f"{community_mapping_duplicates}"
    )

    if province_mapping_duplicates != 0:
        raise RuntimeError(
            "At least one ESIOS province source name "
            "maps to multiple canonical provinces."
        )

    if community_mapping_duplicates != 0:
        raise RuntimeError(
            "At least one ESIOS CCAA source name "
            "maps to multiple canonical communities."
        )

    print("=" * 80)
    print(
        "ALL GOLD GEOGRAPHICAL NORMALIZATION VALIDATED"
    )
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()