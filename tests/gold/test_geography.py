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
    aggregate_hourly_scalars_points_to_province,
    aggregate_hourly_wind_points_to_province,
    prepare_cnig_autonomous_communities,
    validate_required_columns,
    add_deterministic_geography_key,
)

from gold.temporal import (
    aggregate_open_meteo_wind_to_hourly_point,
)




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

    open_meteo_15min = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_OPEN_METEO_15MIN,
    )

    open_meteo_hourly = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_OPEN_METEO_HOURLY,
    )

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

    hourly_wind_duplicates = count_duplicate_keys(
        hourly_wind_province,
        [
            "province_code",
            "gold_timestamp",
        ],
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
        f"ROWS = {hourly_wind_province.count()}"
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

    hourly_scalars_province = (
        aggregate_hourly_scalars_points_to_province(
            open_meteo_hourly,
            metric_columns=HOURLY_SCALAR_METRICS,
        )
    )

    hourly_scalar_duplicates = count_duplicate_keys(
        hourly_scalars_province,
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    print("-" * 80)
    print("HOURLY_SCALARS_POINT_TO_PROVINCE")
    print(
        f"ROWS = {hourly_scalars_province.count()}"
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

    print("=" * 80)
    print(
        "ALL ACTIVE GOLD GEOGRAPHICAL "
        "TRANSFORMATIONS VALIDATED"
    )
    print("=" * 80)

    spark.stop()



def test_deterministic_geography_key_is_stable_for_same_province(
    spark,
):
    df = spark.createDataFrame(
        [
            ("20",),
            ("20",),
        ],
        [
            "province_code",
        ],
    )

    result = (
        add_deterministic_geography_key(
            df,
            geography_level="PROVINCE",
            geography_code_column="province_code",
        )
        .select(
            "geography_key"
        )
        .distinct()
        .collect()
    )

    assert len(result) == 1
    assert len(result[0]["geography_key"]) == 64


def test_deterministic_geography_key_changes_for_different_provinces(
    spark,
):
    df = spark.createDataFrame(
        [
            ("20",),
            ("28",),
        ],
        [
            "province_code",
        ],
    )

    result = (
        add_deterministic_geography_key(
            df,
            geography_level="PROVINCE",
            geography_code_column="province_code",
        )
        .select(
            "geography_key"
        )
        .distinct()
        .collect()
    )

    assert len(result) == 2


def test_deterministic_geography_key_separates_province_and_ccaa(
    spark,
):
    province_df = spark.createDataFrame(
        [
            ("01",),
        ],
        [
            "code",
        ],
    )

    ccaa_df = spark.createDataFrame(
        [
            ("01",),
        ],
        [
            "code",
        ],
    )

    province_key = (
        add_deterministic_geography_key(
            province_df,
            geography_level="PROVINCE",
            geography_code_column="code",
        )
        .first()["geography_key"]
    )

    ccaa_key = (
        add_deterministic_geography_key(
            ccaa_df,
            geography_level="AUTONOMOUS_COMMUNITY",
            geography_code_column="code",
        )
        .first()["geography_key"]
    )

    assert province_key != ccaa_key

if __name__ == "__main__":
    main()