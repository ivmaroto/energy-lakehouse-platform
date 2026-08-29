from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.common import (
    TABLE_GOLD_DIM_GEOGRAPHY,
    TABLE_GOLD_DIM_TIME,
    TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY,
    TABLE_GOLD_FACT_PROVINCE_HOURLY,
    get_spark_session,
)


# ============================================================================
# Helpers
# ============================================================================

def validate_unique_key(
    df: DataFrame,
    key_columns: list[str],
    dataset_name: str,
) -> None:
    duplicate_groups = (
        df.groupBy(
            *key_columns
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(
        f"{dataset_name} "
        f"DUPLICATE_KEY_GROUPS = {duplicate_groups}"
    )

    if duplicate_groups != 0:
        raise RuntimeError(
            f"{dataset_name} contains duplicate natural/business keys "
            f"for {key_columns}."
        )


def validate_no_null_key(
    df: DataFrame,
    key_column: str,
    dataset_name: str,
) -> None:
    null_count = (
        df.filter(
            F.col(key_column).isNull()
        )
        .count()
    )

    print(
        f"{dataset_name} "
        f"NULL_{key_column.upper()} = {null_count}"
    )

    if null_count != 0:
        raise RuntimeError(
            f"{dataset_name} contains NULL {key_column} values."
        )


def validate_foreign_key(
    fact_df: DataFrame,
    dimension_df: DataFrame,
    fact_key: str,
    dimension_key: str,
    dataset_name: str,
    dimension_name: str,
) -> None:
    unmatched_count = (
        fact_df.select(
            fact_key
        )
        .distinct()
        .join(
            dimension_df.select(
                F.col(
                    dimension_key
                ).alias(
                    fact_key
                )
            )
            .distinct(),
            on=fact_key,
            how="left_anti",
        )
        .count()
    )

    print(
        f"{dataset_name} -> {dimension_name} "
        f"UNMATCHED_{fact_key.upper()} = {unmatched_count}"
    )

    if unmatched_count != 0:
        raise RuntimeError(
            f"{dataset_name} contains {unmatched_count} "
            f"{fact_key} values not present in {dimension_name}."
        )


# ============================================================================
# Main validation
# ============================================================================

def main() -> None:
    spark = get_spark_session(
        "test-gold-model-integration"
    )

    try:
        print("=" * 80)
        print("TEST CURRENT GOLD MODEL INTEGRATION")
        print("=" * 80)

        # --------------------------------------------------------------------
        # Physical inventory
        # --------------------------------------------------------------------

        table_rows = (
            spark.sql(
                "SHOW TABLES IN lakehouse.gold"
            )
            .collect()
        )

        existing_tables = {
            row["tableName"]
            for row in table_rows
        }

        expected_tables = {
            TABLE_GOLD_FACT_PROVINCE_HOURLY.split(".")[-1],
            TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY.split(".")[-1],
            TABLE_GOLD_DIM_TIME.split(".")[-1],
            TABLE_GOLD_DIM_GEOGRAPHY.split(".")[-1],
        }

        missing_tables = sorted(
            expected_tables
            - existing_tables
        )

        unexpected_tables = sorted(
            existing_tables
            - expected_tables
        )

        print(
            f"EXPECTED_TABLES = {len(expected_tables)}"
        )
        print(
            f"EXISTING_TABLES = {len(existing_tables)}"
        )
        print(
            f"MISSING_TABLES = {missing_tables}"
        )
        print(
            f"UNEXPECTED_TABLES = {unexpected_tables}"
        )

        if missing_tables or unexpected_tables:
            raise RuntimeError(
                "Gold physical inventory does not match the approved "
                "four-table model."
            )

        # --------------------------------------------------------------------
        # Read current Gold
        # --------------------------------------------------------------------

        province_hourly = spark.table(
            TABLE_GOLD_FACT_PROVINCE_HOURLY
        )

        installed_capacity = spark.table(
            TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY
        )

        dim_time = spark.table(
            TABLE_GOLD_DIM_TIME
        )

        dim_geography = spark.table(
            TABLE_GOLD_DIM_GEOGRAPHY
        )

        print("-" * 80)
        print("ROW COUNTS")
        print("-" * 80)

        print(
            f"{TABLE_GOLD_FACT_PROVINCE_HOURLY} = "
            f"{province_hourly.count()}"
        )
        print(
            f"{TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY} = "
            f"{installed_capacity.count()}"
        )
        print(
            f"{TABLE_GOLD_DIM_TIME} = "
            f"{dim_time.count()}"
        )
        print(
            f"{TABLE_GOLD_DIM_GEOGRAPHY} = "
            f"{dim_geography.count()}"
        )

        # --------------------------------------------------------------------
        # Null keys
        # --------------------------------------------------------------------

        print("-" * 80)
        print("NULL KEY VALIDATION")
        print("-" * 80)

        validate_no_null_key(
            province_hourly,
            "time_key",
            "gold_fact_province_hourly",
        )
        validate_no_null_key(
            province_hourly,
            "geography_key",
            "gold_fact_province_hourly",
        )

        validate_no_null_key(
            installed_capacity,
            "time_key",
            "gold_fact_installed_capacity_monthly",
        )
        validate_no_null_key(
            installed_capacity,
            "geography_key",
            "gold_fact_installed_capacity_monthly",
        )

        validate_no_null_key(
            dim_time,
            "time_key",
            "gold_dim_time",
        )
        validate_no_null_key(
            dim_geography,
            "geography_key",
            "gold_dim_geography",
        )

        # --------------------------------------------------------------------
        # Business/natural key uniqueness
        # --------------------------------------------------------------------

        print("-" * 80)
        print("KEY UNIQUENESS")
        print("-" * 80)

        validate_unique_key(
            province_hourly,
            [
                "geography_key",
                "gold_timestamp",
            ],
            "gold_fact_province_hourly",
        )

        validate_unique_key(
            installed_capacity,
            [
                "geography_key",
                "year_month",
            ],
            "gold_fact_installed_capacity_monthly",
        )

        validate_unique_key(
            dim_time,
            [
                "time_key",
            ],
            "gold_dim_time",
        )

        validate_unique_key(
            dim_geography,
            [
                "geography_key",
            ],
            "gold_dim_geography",
        )

        # --------------------------------------------------------------------
        # Referential integrity
        # --------------------------------------------------------------------

        print("-" * 80)
        print("REFERENTIAL INTEGRITY")
        print("-" * 80)

        validate_foreign_key(
            province_hourly,
            dim_time,
            "time_key",
            "time_key",
            "gold_fact_province_hourly",
            "gold_dim_time",
        )

        validate_foreign_key(
            installed_capacity,
            dim_time,
            "time_key",
            "time_key",
            "gold_fact_installed_capacity_monthly",
            "gold_dim_time",
        )

        validate_foreign_key(
            province_hourly,
            dim_geography,
            "geography_key",
            "geography_key",
            "gold_fact_province_hourly",
            "gold_dim_geography",
        )

        validate_foreign_key(
            installed_capacity,
            dim_geography,
            "geography_key",
            "geography_key",
            "gold_fact_installed_capacity_monthly",
            "gold_dim_geography",
        )

        # --------------------------------------------------------------------
        # Dimension composition
        # --------------------------------------------------------------------

        print("-" * 80)
        print("DIMENSION COMPOSITION")
        print("-" * 80)

        time_grains = (
            dim_time.groupBy(
                "time_grain"
            )
            .count()
            .orderBy(
                "time_grain"
            )
            .collect()
        )

        for row in time_grains:
            print(
                f"DIM_TIME_GRAIN "
                f"{row['time_grain']} = {row['count']}"
            )

        geography_levels = (
            dim_geography.groupBy(
                "geography_level"
            )
            .count()
            .orderBy(
                "geography_level"
            )
            .collect()
        )

        for row in geography_levels:
            print(
                f"DIM_GEOGRAPHY_LEVEL "
                f"{row['geography_level']} = {row['count']}"
            )

        invalid_time_grains = (
            dim_time.filter(
                ~F.col(
                    "time_grain"
                ).isin(
                    "HOUR",
                    "MONTH",
                )
            )
            .count()
        )

        invalid_geography_levels = (
            dim_geography.filter(
                ~F.col(
                    "geography_level"
                ).isin(
                    "PROVINCE",
                    "AUTONOMOUS_COMMUNITY",
                )
            )
            .count()
        )

        print(
            f"INVALID_TIME_GRAIN_ROWS = "
            f"{invalid_time_grains}"
        )
        print(
            f"INVALID_GEOGRAPHY_LEVEL_ROWS = "
            f"{invalid_geography_levels}"
        )

        if invalid_time_grains != 0:
            raise RuntimeError(
                "gold_dim_time contains retired/unexpected time grains."
            )

        if invalid_geography_levels != 0:
            raise RuntimeError(
                "gold_dim_geography contains retired/unexpected "
                "geography levels."
            )

        print("=" * 80)
        print(
            "GOLD MODEL INTEGRATION TEST PASSED"
        )
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
