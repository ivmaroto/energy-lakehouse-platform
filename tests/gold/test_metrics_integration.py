from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold.metrics import (
    HOURLY_ENERGY_METRICS,
    INSTALLED_CAPACITY_METRICS,
    prepare_hourly_energy_metrics,
    prepare_installed_capacity_metrics,
    select_approved_indicators,
)

from gold.geography import (
    COUNTRY_ES_GEOGRAPHY_KEY,
    PENINSULA_ES_GEOGRAPHY_KEY,
)

# ============================================================================
# Iceberg Silver sources
# ============================================================================

TABLE_ESIOS_ENERGY_HOURLY = (
    "lakehouse.silver.silver_esios_energy_hourly"
)

TABLE_ESIOS_INSTALLED_CAPACITY = (
    "lakehouse.silver."
    "silver_esios_installed_capacity_monthly"
)



# ============================================================================
# Helpers
# ============================================================================

def assert_table_exists(
    spark: SparkSession,
    table_name: str,
) -> None:
    """
    Integration validation requires the real persisted Silver table.
    """
    if not spark.catalog.tableExists(
        table_name
    ):
        raise AssertionError(
            f"Required Silver table does not exist: "
            f"{table_name}"
        )


def assert_required_columns(
    df: DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> None:
    """
    Fail explicitly if the persisted Silver schema does not contain the
    structural columns required by the Gold metric transformation.
    """
    missing = sorted(
        required_columns
        - set(df.columns)
    )

    if missing:
        raise AssertionError(
            f"{dataset_name} missing columns: "
            f"{missing}"
        )


def collect_distinct_indicator_ids(
    df: DataFrame,
) -> set[int]:
    """
    Return all non-null indicator IDs contained in a real Silver DataFrame.
    """
    return {
        int(
            row["indicator_id"]
        )
        for row
        in (
            df
            .select(
                "indicator_id"
            )
            .where(
                F.col(
                    "indicator_id"
                ).isNotNull()
            )
            .distinct()
            .collect()
        )
    }


def validate_all_approved_ids_present(
    actual_ids: set[int],
    expected_ids: set[int],
    dataset_name: str,
) -> None:
    """
    Every indicator approved for the Gold product must exist in the real
    persisted Silver source used by this integration test.
    """
    missing = sorted(
        expected_ids
        - actual_ids
    )

    if missing:
        raise AssertionError(
            f"{dataset_name} does not contain all approved "
            f"Gold indicators. Missing IDs: {missing}"
        )


def validate_result_columns(
    df: DataFrame,
    expected_metric_columns: set[str],
    dataset_name: str,
) -> None:
    """
    Validate that every approved Gold metric has been materialized as a
    physical output column.
    """
    missing = sorted(
        expected_metric_columns
        - set(df.columns)
    )

    if missing:
        raise AssertionError(
            f"{dataset_name} missing Gold metrics: "
            f"{missing}"
        )


def validate_unique_rows(
    df: DataFrame,
    grain_columns: list[str],
    dataset_name: str,
) -> None:
    """
    Validate final uniqueness at the metric-product grain.
    """
    duplicate_count = (
        df
        .groupBy(
            *grain_columns
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_count != 0:
        raise AssertionError(
            f"{dataset_name} contains "
            f"{duplicate_count} duplicated Gold grains."
        )


def print_validation_block(
    title: str,
    *,
    source_rows: int,
    approved_source_rows: int,
    result_rows: int,
    source_indicator_count: int,
    approved_indicator_count: int,
) -> None:
    print("-" * 80)
    print(title)
    print(
        f"SOURCE_ROWS = {source_rows}"
    )
    print(
        f"APPROVED_SOURCE_ROWS = "
        f"{approved_source_rows}"
    )
    print(
        f"RESULT_ROWS = {result_rows}"
    )
    print(
        f"SOURCE_DISTINCT_INDICATORS = "
        f"{source_indicator_count}"
    )
    print(
        f"APPROVED_INDICATORS = "
        f"{approved_indicator_count}"
    )


# ============================================================================
# Real Silver -> Gold hourly metric validation
# ============================================================================

def validate_hourly_energy_metrics(
    spark: SparkSession,
) -> None:
    source = spark.table(
        TABLE_ESIOS_ENERGY_HOURLY
    )

    assert_required_columns(
        source,
        {
            "indicator_id",
            "observation_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "value",
        },
        TABLE_ESIOS_ENERGY_HOURLY,
    )

    source_ids = (
        collect_distinct_indicator_ids(
            source
        )
    )

    approved_ids = set(
        HOURLY_ENERGY_METRICS
    )

    validate_all_approved_ids_present(
        source_ids,
        approved_ids,
        TABLE_ESIOS_ENERGY_HOURLY,
    )

    # ------------------------------------------------------------------------
    # Metrics integration test only.
    #
    # Temporal alignment itself is already validated by temporal.py tests.
    # Here the existing source timestamp is used only to provide the
    # gold_timestamp structural column required by metrics.py.
    # ------------------------------------------------------------------------

    prepared_source = (
        source
        .withColumn(
            "gold_timestamp",
            F.col(
                "observation_timestamp"
            ),
        )
    )

    approved_source = (
        select_approved_indicators(
            prepared_source,
            HOURLY_ENERGY_METRICS,
            dataset_name=(
                "real Silver hourly ESIOS"
            ),
        )
    )

    result = (
        prepare_hourly_energy_metrics(
            prepared_source
        )
    )

    validate_result_columns(
        result,
        set(
            HOURLY_ENERGY_METRICS.values()
        ),
        "Gold hourly metrics integration",
    )

    validate_unique_rows(
        result,
        [
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "gold_timestamp",
        ],
        "Gold hourly metrics integration",
    )

    # No raw indicator columns should survive the pivot.
    assert (
        "indicator_id"
        not in result.columns
    )

    assert (
        "value"
        not in result.columns
    )

    source_rows = source.count()
    approved_rows = approved_source.count()
    result_rows = result.count()

    if approved_rows == 0:
        raise AssertionError(
            "No approved hourly ESIOS observations "
            "were found in real Silver."
        )

    if result_rows == 0:
        raise AssertionError(
            "Hourly Gold metric preparation "
            "produced zero rows."
        )

    print_validation_block(
        "ESIOS HOURLY GOLD METRICS",
        source_rows=source_rows,
        approved_source_rows=approved_rows,
        result_rows=result_rows,
        source_indicator_count=len(
            source_ids
        ),
        approved_indicator_count=len(
            approved_ids
        ),
    )


# ============================================================================
# Real Silver -> Gold installed-capacity metric validation
# ============================================================================

def validate_installed_capacity_metrics(
    spark: SparkSession,
) -> None:
    source = spark.table(
        TABLE_ESIOS_INSTALLED_CAPACITY
    )

    assert_required_columns(
        source,
        {
            "indicator_id",
            "observation_timestamp",
            "esios_geo_id",
            "autonomous_community_code",
            "autonomous_community_name",
            "value",
        },
        TABLE_ESIOS_INSTALLED_CAPACITY,
    )

    source_ids = (
        collect_distinct_indicator_ids(
            source
        )
    )

    approved_ids = set(
        INSTALLED_CAPACITY_METRICS
    )

    validate_all_approved_ids_present(
        source_ids,
        approved_ids,
        TABLE_ESIOS_INSTALLED_CAPACITY,
    )

    # ------------------------------------------------------------------------
    # Monthly capacity has no automatic +1-hour correction.
    #
    # The structural Gold month fields are derived directly from the real
    # persisted Silver observation timestamp for this metrics integration
    # validation.
    # ------------------------------------------------------------------------

    prepared_source = (
        source
        .withColumn(
            "source_timestamp",
            F.col(
                "observation_timestamp"
            ),
        )
        .withColumn(
            "gold_month_timestamp",
            F.date_trunc(
                "month",
                F.col(
                    "observation_timestamp"
                ),
            ),
        )
        .withColumn(
            "year_month",
            F.date_format(
                F.col(
                    "observation_timestamp"
                ),
                "yyyy-MM",
            ),
        )
    )

    approved_source = (
        select_approved_indicators(
            prepared_source,
            INSTALLED_CAPACITY_METRICS,
            dataset_name=(
                "real Silver installed capacity"
            ),
        )
    )

    result = (
        prepare_installed_capacity_metrics(
            prepared_source
        )
    )

    validate_result_columns(
        result,
        set(
            INSTALLED_CAPACITY_METRICS.values()
        ),
        (
            "Gold installed-capacity "
            "metrics integration"
        ),
    )

    validate_unique_rows(
        result,
        [
            "year_month",
            "gold_month_timestamp",
            "source_timestamp",
            "autonomous_community_code",
            "autonomous_community_name",
            "esios_geo_id",
        ],
        (
            "Gold installed-capacity "
            "metrics integration"
        ),
    )

    if (
        result
        .filter(
            F.col(
                "autonomous_community_code"
            ).isNull()
            |
            F.col(
                "autonomous_community_name"
            ).isNull()
        )
        .count()
        != 0
    ):
        raise AssertionError(
            "Installed-capacity Gold metrics contain "
            "NULL canonical CCAA geography."
        )

    source_rows = source.count()
    approved_rows = approved_source.count()
    result_rows = result.count()

    if approved_rows == 0:
        raise AssertionError(
            "No approved installed-capacity ESIOS "
            "observations were found in real Silver."
        )

    if result_rows == 0:
        raise AssertionError(
            "Installed-capacity Gold metric preparation "
            "produced zero rows."
        )

    print_validation_block(
        "ESIOS INSTALLED CAPACITY GOLD METRICS",
        source_rows=source_rows,
        approved_source_rows=approved_rows,
        result_rows=result_rows,
        source_indicator_count=len(
            source_ids
        ),
        approved_indicator_count=len(
            approved_ids
        ),
    )


# ============================================================================
# Real Silver -> Gold 5-minute metric validation
# ============================================================================



# ============================================================================
# Main integration validation
# ============================================================================

def main() -> None:
    spark = (
        SparkSession.builder
        .appName(
            "gold-metrics-integration-validation"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("VALIDATE GOLD METRICS AGAINST REAL SILVER")
    print("=" * 80)

    assert_table_exists(
        spark,
        TABLE_ESIOS_ENERGY_HOURLY,
    )

    assert_table_exists(
        spark,
        TABLE_ESIOS_INSTALLED_CAPACITY,
    )

    validate_hourly_energy_metrics(
        spark
    )

    validate_installed_capacity_metrics(
        spark
    )

    print("=" * 80)
    print(
        "ALL ACTIVE GOLD METRICS "
        "INTEGRATION VALIDATED"
    )
    print("=" * 80)

    spark.stop()



if __name__ == "__main__":
    main()