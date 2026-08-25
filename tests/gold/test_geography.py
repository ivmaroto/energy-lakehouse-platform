from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from gold.common import (
    TABLE_SILVER_OPEN_METEO_15MIN,
    TABLE_SILVER_OPEN_METEO_HOURLY,
    get_spark_session,
    read_silver_table,
)

from gold.geography import (
    COUNTRY_ES_GEOGRAPHY_KEY,
    PENINSULA_ES_GEOGRAPHY_KEY,
    aggregate_15min_directions_points_to_province,
    aggregate_15min_directions_province_to_spain,
    aggregate_15min_scalars_points_to_province,
    aggregate_15min_scalars_province_to_spain,
    aggregate_hourly_scalars_points_to_province,
    aggregate_hourly_wind_points_to_province,
    prepare_cnig_autonomous_communities,
    validate_required_columns,
)

from gold.temporal import (
    aggregate_open_meteo_wind_to_hourly_point,
)


SCALAR_15MIN_METRICS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_80m",
    "wind_speed_120m",
]


HOURLY_SCALAR_METRICS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "shortwave_radiation",
    "direct_normal_irradiance",
]

# ============================================================================
# Pytest Spark fixture
# ============================================================================

@pytest.fixture(scope="module")
def spark():
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("gold-geography-unit-tests")
        .config(
            "spark.pyspark.python",
            python_executable,
        )
        .config(
            "spark.pyspark.driver.python",
            python_executable,
        )
        .getOrCreate()
    )

    yield session


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

# ============================================================================
# Unit tests
# ============================================================================

def test_gold_country_geography_keys_are_canonical():
    assert (
        COUNTRY_ES_GEOGRAPHY_KEY
        == "COUNTRY:ES"
    )

    assert (
        PENINSULA_ES_GEOGRAPHY_KEY
        == "PENINSULA:ES-PEN"
    )


def test_validate_required_columns_rejects_missing_geographical_column(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "13",
            ),
        ],
        [
            "autonomous_community_code",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing required",
    ):
        validate_required_columns(
            df,
            {
                "autonomous_community_code",
                "autonomous_community_name",
            },
            "test autonomous communities",
        )


def test_prepare_cnig_autonomous_communities_accepts_unique_codes(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "13",
                "Comunidad de Madrid",
            ),
            (
                "16",
                "País Vasco/Euskadi",
            ),
        ],
        [
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    result = (
        prepare_cnig_autonomous_communities(
            df
        )
    )

    assert result.count() == 2

    assert (
        result
        .select(
            "autonomous_community_code"
        )
        .distinct()
        .count()
        == 2
    )

    assert (
        "_cnig_normalized_autonomous_community"
        in result.columns
    )


def test_prepare_cnig_autonomous_communities_rejects_duplicate_code(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "13",
                "Comunidad de Madrid",
            ),
            (
                "13",
                "OTRO NOMBRE",
            ),
        ],
        [
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicated autonomous_community_code",
    ):
        prepare_cnig_autonomous_communities(
            df
        )


def main() -> None:
    spark = get_spark_session(
        "gold-validate-geographical-transformations"
    )

    print("=" * 80)
    print("VALIDATE GOLD GEOGRAPHICAL TRANSFORMATIONS")
    print("=" * 80)

    # ========================================================================
    # Read Silver
    # ========================================================================

    open_meteo_15min = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_OPEN_METEO_15MIN,
    )

    open_meteo_hourly = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_OPEN_METEO_HOURLY,
    )

    # ========================================================================
    # Province × hour - wind
    # ========================================================================

    hourly_wind_point = (
        aggregate_open_meteo_wind_to_hourly_point(
            open_meteo_15min
        )
    )

    hourly_wind_province = (
        aggregate_hourly_wind_points_to_province(
            hourly_wind_point
        )
    )

    hourly_wind_rows = (
        hourly_wind_province
        .count()
    )

    hourly_wind_duplicates = (
        count_duplicate_keys(
            hourly_wind_province,
            [
                "province_code",
                "gold_timestamp",
            ],
        )
    )

    hourly_wind_null_province = (
        hourly_wind_province
        .filter(
            F.col("province_code").isNull()
        )
        .count()
    )

    print("-" * 80)
    print("HOURLY_WIND_POINT_TO_PROVINCE")
    print(
        f"ROWS = {hourly_wind_rows}"
    )
    print(
        f"DUPLICATE_PROVINCE_HOUR_KEYS = "
        f"{hourly_wind_duplicates}"
    )
    print(
        f"NULL_PROVINCE_CODES = "
        f"{hourly_wind_null_province}"
    )

    if hourly_wind_duplicates != 0:
        raise RuntimeError(
            "Hourly wind province aggregation produced "
            "duplicate province/hour keys."
        )

    if hourly_wind_null_province != 0:
        raise RuntimeError(
            "Hourly wind province aggregation contains "
            "NULL province codes."
        )

    # ========================================================================
    # Province × hour - scalar Open-Meteo
    # ========================================================================

    hourly_scalars_province = (
        aggregate_hourly_scalars_points_to_province(
            open_meteo_hourly,
            metric_columns=HOURLY_SCALAR_METRICS,
        )
    )

    hourly_scalar_rows = (
        hourly_scalars_province
        .count()
    )

    hourly_scalar_duplicates = (
        count_duplicate_keys(
            hourly_scalars_province,
            [
                "province_code",
                "gold_timestamp",
            ],
        )
    )

    print("-" * 80)
    print("HOURLY_SCALARS_POINT_TO_PROVINCE")
    print(
        f"ROWS = {hourly_scalar_rows}"
    )
    print(
        f"DUPLICATE_PROVINCE_HOUR_KEYS = "
        f"{hourly_scalar_duplicates}"
    )

    if hourly_scalar_duplicates != 0:
        raise RuntimeError(
            "Hourly scalar province aggregation produced "
            "duplicate province/hour keys."
        )

    # ========================================================================
    # 15 min - point -> province
    # ========================================================================

    province_scalars_15min = (
        aggregate_15min_scalars_points_to_province(
            open_meteo_15min,
            metric_columns=SCALAR_15MIN_METRICS,
        )
    )

    province_directions_15min = (
        aggregate_15min_directions_points_to_province(
            open_meteo_15min
        )
    )

    province_scalar_rows = (
        province_scalars_15min
        .count()
    )

    province_direction_rows = (
        province_directions_15min
        .count()
    )

    province_scalar_duplicates = (
        count_duplicate_keys(
            province_scalars_15min,
            [
                "province_code",
                "gold_timestamp",
            ],
        )
    )

    province_direction_duplicates = (
        count_duplicate_keys(
            province_directions_15min,
            [
                "province_code",
                "gold_timestamp",
            ],
        )
    )

    print("-" * 80)
    print("15MIN_POINT_TO_PROVINCE")
    print(
        f"SCALAR_ROWS = {province_scalar_rows}"
    )
    print(
        f"DIRECTION_ROWS = {province_direction_rows}"
    )
    print(
        f"SCALAR_DUPLICATES = "
        f"{province_scalar_duplicates}"
    )
    print(
        f"DIRECTION_DUPLICATES = "
        f"{province_direction_duplicates}"
    )

    if province_scalar_duplicates != 0:
        raise RuntimeError(
            "15-minute scalar province aggregation "
            "produced duplicate keys."
        )

    if province_direction_duplicates != 0:
        raise RuntimeError(
            "15-minute direction province aggregation "
            "produced duplicate keys."
        )

    # ========================================================================
    # 15 min - province -> Spain
    # ========================================================================

    spain_scalars_15min = (
        aggregate_15min_scalars_province_to_spain(
            province_scalars_15min,
            metric_columns=SCALAR_15MIN_METRICS,
        )
    )

    spain_directions_15min = (
        aggregate_15min_directions_province_to_spain(
            province_directions_15min
        )
    )

    spain_scalar_rows = (
        spain_scalars_15min
        .count()
    )

    spain_direction_rows = (
        spain_directions_15min
        .count()
    )

    spain_scalar_duplicates = (
        count_duplicate_keys(
            spain_scalars_15min,
            [
                "gold_timestamp",
            ],
        )
    )

    spain_direction_duplicates = (
        count_duplicate_keys(
            spain_directions_15min,
            [
                "gold_timestamp",
            ],
        )
    )

    invalid_scalar_geography = (
        spain_scalars_15min
        .filter(
            (F.col("geography_level") != "COUNTRY")
            |
            (F.col("geography_name") != "España")
        )
        .count()
    )

    invalid_direction_geography = (
        spain_directions_15min
        .filter(
            (F.col("geography_level") != "COUNTRY")
            |
            (F.col("geography_name") != "España")
        )
        .count()
    )

    print("-" * 80)
    print("15MIN_PROVINCE_TO_SPAIN")
    print(
        f"SCALAR_ROWS = {spain_scalar_rows}"
    )
    print(
        f"DIRECTION_ROWS = {spain_direction_rows}"
    )
    print(
        f"SCALAR_DUPLICATES = "
        f"{spain_scalar_duplicates}"
    )
    print(
        f"DIRECTION_DUPLICATES = "
        f"{spain_direction_duplicates}"
    )
    print(
        f"INVALID_SCALAR_GEOGRAPHY = "
        f"{invalid_scalar_geography}"
    )
    print(
        f"INVALID_DIRECTION_GEOGRAPHY = "
        f"{invalid_direction_geography}"
    )

    if spain_scalar_duplicates != 0:
        raise RuntimeError(
            "National scalar aggregation produced "
            "duplicate timestamps."
        )

    if spain_direction_duplicates != 0:
        raise RuntimeError(
            "National direction aggregation produced "
            "duplicate timestamps."
        )

    if invalid_scalar_geography != 0:
        raise RuntimeError(
            "Invalid national geography detected "
            "in scalar aggregation."
        )

    if invalid_direction_geography != 0:
        raise RuntimeError(
            "Invalid national geography detected "
            "in direction aggregation."
        )

    print("=" * 80)
    print(
        "ALL GOLD GEOGRAPHICAL TRANSFORMATIONS VALIDATED"
    )
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()