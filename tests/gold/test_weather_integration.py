from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold.weather import (
    COUNTRY_15MIN_WEATHER_METRICS,
    PROVINCE_HOURLY_WEATHER_METRICS,
    prepare_aemet_province_hourly,
    prepare_country_15min_weather,
    prepare_open_meteo_province_15min,
    prepare_open_meteo_province_hourly,
    prepare_open_meteo_wind_point_hourly,
    prepare_open_meteo_wind_province_hourly,
    prepare_province_hourly_weather,
    prepare_peninsula_15min_weather,
)


# ============================================================================
# Real Silver Iceberg sources
# ============================================================================

TABLE_AEMET_CURRENT = (
    "lakehouse.silver."
    "silver_aemet_current_observations"
)

TABLE_AEMET_STATIONS = (
    "lakehouse.silver."
    "silver_aemet_stations"
)

TABLE_OPEN_METEO_HOURLY = (
    "lakehouse.silver."
    "silver_open_meteo_hourly"
)

TABLE_OPEN_METEO_15MIN = (
    "lakehouse.silver."
    "silver_open_meteo_15min"
)


# ============================================================================
# Integration-test constants
#
# The final literal serialization of geography_key is intentionally not
# decided here.
#
# weather.py receives geography_key from the Gold geographical model.
# This value is therefore test-only and validates that the function preserves
# the supplied deterministic key without manufacturing another one.
# ============================================================================

TEST_COUNTRY_GEOGRAPHY_KEY = (
    "TEST_COUNTRY_ES"
)

TEST_PENINSULA_GEOGRAPHY_KEY = (
    "TEST_PENINSULA_ES"
)

GOLD_CONFIG_PATH = Path(
    "/opt/config/gold_config.json"
)

# ============================================================================
# Gold configuration helpers
# ============================================================================

def load_peninsula_excluded_province_codes() -> list[str]:
    """
    Load the validated non-peninsular CNIG province codes from Gold config.

    The geographical scope must not be duplicated or hardcoded inside the
    weather transformation or its integration validation.
    """
    if not GOLD_CONFIG_PATH.exists():
        raise AssertionError(
            "Gold configuration file does not exist: "
            f"{GOLD_CONFIG_PATH}"
        )

    with GOLD_CONFIG_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = json.load(
            file
        )

    excluded_codes = config.get(
        "peninsula_excluded_province_codes"
    )

    if not isinstance(
        excluded_codes,
        list,
    ):
        raise AssertionError(
            "Gold configuration "
            "'peninsula_excluded_province_codes' "
            "must be a list."
        )

    if not all(
        isinstance(
            code,
            str,
        )
        for code in excluded_codes
    ):
        raise AssertionError(
            "Every peninsula excluded province code "
            "must be a string."
        )

    if len(
        excluded_codes
    ) != len(
        set(
            excluded_codes
        )
    ):
        raise AssertionError(
            "Peninsula excluded province codes "
            "contain duplicates."
        )

    return excluded_codes


# ============================================================================
# Generic helpers
# ============================================================================

def assert_table_exists(
    spark: SparkSession,
    table_name: str,
) -> None:
    """
    Real integration validation requires the persisted Iceberg Silver table.
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
    Fail explicitly if the persisted Silver schema is not compatible with the
    approved Gold weather transformation.
    """
    missing = sorted(
        required_columns
        - set(df.columns)
    )

    if missing:
        raise AssertionError(
            f"{dataset_name} missing required columns: "
            f"{missing}"
        )


def validate_non_empty(
    df: DataFrame,
    dataset_name: str,
) -> int:
    """
    Integration products based on reproducible Open-Meteo Silver data must
    contain real rows.
    """
    row_count = df.count()

    if row_count == 0:
        raise AssertionError(
            f"{dataset_name} produced zero rows."
        )

    return row_count


def validate_unique_grain(
    df: DataFrame,
    grain_columns: list[str],
    dataset_name: str,
) -> None:
    """
    Validate that no transformation hides or introduces duplicates at the
    approved analytical grain.
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
            f"{duplicate_count} duplicated grains."
        )


def validate_structural_columns_not_null(
    df: DataFrame,
    columns: list[str],
    dataset_name: str,
) -> None:
    """
    Gold structural grain columns cannot be NULL.
    """
    condition = None

    for column in columns:
        current = F.col(
            column
        ).isNull()

        if condition is None:
            condition = current
        else:
            condition = (
                condition
                |
                current
            )

    null_count = (
        df
        .filter(
            condition
        )
        .count()
    )

    if null_count != 0:
        raise AssertionError(
            f"{dataset_name} contains "
            f"{null_count} rows with NULL structural fields."
        )


def validate_metric_columns_exist(
    df: DataFrame,
    metric_columns: tuple[str, ...],
    dataset_name: str,
) -> None:
    """
    Validate the complete approved Gold weather metric contract.
    """
    missing = sorted(
        set(
            metric_columns
        )
        - set(
            df.columns
        )
    )

    if missing:
        raise AssertionError(
            f"{dataset_name} missing metrics: "
            f"{missing}"
        )


def validate_wind_directions(
    df: DataFrame,
    dataset_name: str,
) -> None:
    """
    Every non-null circular-mean direction must remain inside [0, 360).
    """
    invalid_count = (
        df
        .filter(
            (
                F.col(
                    "wind_direction_80m"
                ).isNotNull()
                &
                (
                    (
                        F.col(
                            "wind_direction_80m"
                        )
                        < F.lit(0.0)
                    )
                    |
                    (
                        F.col(
                            "wind_direction_80m"
                        )
                        >= F.lit(360.0)
                    )
                )
            )
            |
            (
                F.col(
                    "wind_direction_120m"
                ).isNotNull()
                &
                (
                    (
                        F.col(
                            "wind_direction_120m"
                        )
                        < F.lit(0.0)
                    )
                    |
                    (
                        F.col(
                            "wind_direction_120m"
                        )
                        >= F.lit(360.0)
                    )
                )
            )
        )
        .count()
    )

    if invalid_count != 0:
        raise AssertionError(
            f"{dataset_name} contains "
            f"{invalid_count} wind directions "
            "outside [0, 360)."
        )


def print_validation_block(
    title: str,
    values: dict[str, object],
) -> None:
    print("-" * 80)
    print(title)

    for key, value in values.items():
        print(
            f"{key} = {value}"
        )


# ============================================================================
# Validate real persisted Silver schemas
# ============================================================================

def validate_real_silver_schemas(
    spark: SparkSession,
) -> tuple[
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
]:
    aemet_current = spark.table(
        TABLE_AEMET_CURRENT
    )

    aemet_stations = spark.table(
        TABLE_AEMET_STATIONS
    )

    open_meteo_hourly = spark.table(
        TABLE_OPEN_METEO_HOURLY
    )

    open_meteo_15min = spark.table(
        TABLE_OPEN_METEO_15MIN
    )

    assert_required_columns(
        aemet_current,
        {
            "station_id",
            "observation_timestamp",
            "ta",
            "hr",
            "prec",
        },
        TABLE_AEMET_CURRENT,
    )

    assert_required_columns(
        aemet_stations,
        {
            "station_id",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        },
        TABLE_AEMET_STATIONS,
    )

    assert_required_columns(
        open_meteo_hourly,
        {
            "station_id",
            "observation_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "shortwave_radiation",
            "direct_normal_irradiance",
        },
        TABLE_OPEN_METEO_HOURLY,
    )

    assert_required_columns(
        open_meteo_15min,
        {
            "station_id",
            "observation_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_80m",
            "wind_direction_80m",
            "wind_speed_120m",
            "wind_direction_120m",
            "shortwave_radiation",
            "direct_normal_irradiance",
        },
        TABLE_OPEN_METEO_15MIN,
    )

    print_validation_block(
        "REAL SILVER WEATHER SOURCES",
        {
            "AEMET_CURRENT_ROWS": (
                aemet_current.count()
            ),
            "AEMET_STATIONS_ROWS": (
                aemet_stations.count()
            ),
            "OPEN_METEO_HOURLY_ROWS": (
                open_meteo_hourly.count()
            ),
            "OPEN_METEO_15MIN_ROWS": (
                open_meteo_15min.count()
            ),
        },
    )

    return (
        aemet_current,
        aemet_stations,
        open_meteo_hourly,
        open_meteo_15min,
    )


# ============================================================================
# AEMET real integration
# ============================================================================

def validate_aemet_real(
    aemet_current: DataFrame,
    aemet_stations: DataFrame,
) -> None:
    """
    AEMET current may have a short coverage window by design.

    The validation therefore checks compatibility and correctness without
    imposing a fabricated historical row count.
    """
    source_count = (
        aemet_current.count()
    )

    result = (
        prepare_aemet_province_hourly(
            aemet_current,
            aemet_stations,
        )
    )

    result_count = (
        result.count()
    )

    validate_unique_grain(
        result,
        [
            "province_code",
            "gold_timestamp",
        ],
        "Real AEMET Province × hour",
    )

    if result_count != 0:
        validate_structural_columns_not_null(
            result,
            [
                "province_code",
                "gold_timestamp",
            ],
            "Real AEMET Province × hour",
        )

    print_validation_block(
        "AEMET REAL PROVINCE-HOURLY WEATHER",
        {
            "SOURCE_ROWS": (
                source_count
            ),
            "RESULT_ROWS": (
                result_count
            ),
            "DISTINCT_PROVINCES": (
                result
                .select(
                    "province_code"
                )
                .distinct()
                .count()
            ),
        },
    )


# ============================================================================
# Open-Meteo hourly real integration
# ============================================================================

def validate_open_meteo_hourly_real(
    open_meteo_hourly: DataFrame,
) -> None:
    result = (
        prepare_open_meteo_province_hourly(
            open_meteo_hourly
        )
    )

    result_count = validate_non_empty(
        result,
        (
            "Real Open-Meteo "
            "Province × hour"
        ),
    )

    validate_unique_grain(
        result,
        [
            "province_code",
            "gold_timestamp",
        ],
        (
            "Real Open-Meteo "
            "Province × hour"
        ),
    )

    validate_structural_columns_not_null(
        result,
        [
            "province_code",
            "gold_timestamp",
        ],
        (
            "Real Open-Meteo "
            "Province × hour"
        ),
    )

    print_validation_block(
        "OPEN-METEO REAL PROVINCE-HOURLY WEATHER",
        {
            "SOURCE_ROWS": (
                open_meteo_hourly.count()
            ),
            "RESULT_ROWS": (
                result_count
            ),
            "DISTINCT_PROVINCES": (
                result
                .select(
                    "province_code"
                )
                .distinct()
                .count()
            ),
        },
    )


# ============================================================================
# Open-Meteo real wind 15 min -> hour -> province
# ============================================================================

def validate_open_meteo_wind_real(
    open_meteo_15min: DataFrame,
) -> None:
    point_hourly = (
        prepare_open_meteo_wind_point_hourly(
            open_meteo_15min
        )
    )

    point_hourly_count = validate_non_empty(
        point_hourly,
        (
            "Real Open-Meteo "
            "wind Point × hour"
        ),
    )

    validate_unique_grain(
        point_hourly,
        [
            "station_id",
            "gold_timestamp",
        ],
        (
            "Real Open-Meteo "
            "wind Point × hour"
        ),
    )

    province_hourly = (
        prepare_open_meteo_wind_province_hourly(
            open_meteo_15min
        )
    )

    province_hourly_count = validate_non_empty(
        province_hourly,
        (
            "Real Open-Meteo "
            "wind Province × hour"
        ),
    )

    validate_unique_grain(
        province_hourly,
        [
            "province_code",
            "gold_timestamp",
        ],
        (
            "Real Open-Meteo "
            "wind Province × hour"
        ),
    )

    validate_structural_columns_not_null(
        province_hourly,
        [
            "province_code",
            "gold_timestamp",
        ],
        (
            "Real Open-Meteo "
            "wind Province × hour"
        ),
    )

    validate_wind_directions(
        province_hourly,
        (
            "Real Open-Meteo "
            "wind Province × hour"
        ),
    )

    print_validation_block(
        "OPEN-METEO REAL WIND AGGREGATION",
        {
            "SOURCE_15MIN_ROWS": (
                open_meteo_15min.count()
            ),
            "POINT_HOURLY_ROWS": (
                point_hourly_count
            ),
            "PROVINCE_HOURLY_ROWS": (
                province_hourly_count
            ),
            "DISTINCT_PROVINCES": (
                province_hourly
                .select(
                    "province_code"
                )
                .distinct()
                .count()
            ),
        },
    )


# ============================================================================
# Complete Province × hour weather integration
# ============================================================================

def validate_complete_province_hourly_weather(
    aemet_current: DataFrame,
    aemet_stations: DataFrame,
    open_meteo_hourly: DataFrame,
    open_meteo_15min: DataFrame,
) -> None:
    result = (
        prepare_province_hourly_weather(
            aemet_current,
            aemet_stations,
            open_meteo_hourly,
            open_meteo_15min,
        )
    )

    result_count = validate_non_empty(
        result,
        "Real Gold Province × hour weather",
    )

    validate_metric_columns_exist(
        result,
        PROVINCE_HOURLY_WEATHER_METRICS,
        "Real Gold Province × hour weather",
    )

    validate_unique_grain(
        result,
        [
            "province_code",
            "gold_timestamp",
        ],
        "Real Gold Province × hour weather",
    )

    validate_structural_columns_not_null(
        result,
        [
            "province_code",
            "gold_timestamp",
        ],
        "Real Gold Province × hour weather",
    )

    validate_wind_directions(
        result,
        "Real Gold Province × hour weather",
    )

    # ------------------------------------------------------------------------
    # Source labels may only contain values introduced explicitly by the
    # approved variable-level fallback implementation.
    # ------------------------------------------------------------------------

    for source_column in [
        "temperature_source",
        "humidity_source",
        "precipitation_source",
    ]:
        invalid_source_count = (
            result
            .filter(
                F.col(
                    source_column
                ).isNotNull()
                &
                ~F.col(
                    source_column
                ).isin(
                    [
                        "AEMET",
                        "OPEN_METEO",
                    ]
                )
            )
            .count()
        )

        if invalid_source_count != 0:
            raise AssertionError(
                f"{source_column} contains "
                f"{invalid_source_count} invalid source labels."
            )

    source_distribution = (
        result
        .groupBy(
            "temperature_source"
        )
        .count()
        .orderBy(
            "temperature_source"
        )
        .collect()
    )

    print_validation_block(
        "COMPLETE REAL GOLD PROVINCE-HOURLY WEATHER",
        {
            "RESULT_ROWS": (
                result_count
            ),
            "DISTINCT_PROVINCES": (
                result
                .select(
                    "province_code"
                )
                .distinct()
                .count()
            ),
            "MIN_TIMESTAMP": (
                result
                .agg(
                    F.min(
                        "gold_timestamp"
                    )
                )
                .first()[0]
            ),
            "MAX_TIMESTAMP": (
                result
                .agg(
                    F.max(
                        "gold_timestamp"
                    )
                )
                .first()[0]
            ),
            "TEMPERATURE_SOURCE_DISTRIBUTION": (
                [
                    (
                        row[
                            "temperature_source"
                        ],
                        row["count"],
                    )
                    for row
                    in source_distribution
                ]
            ),
        },
    )


# ============================================================================
# Province × 15 min real integration
# ============================================================================

def validate_province_15min_weather(
    open_meteo_15min: DataFrame,
) -> None:
    result = (
        prepare_open_meteo_province_15min(
            open_meteo_15min
        )
    )

    result_count = validate_non_empty(
        result,
        (
            "Real Open-Meteo "
            "Province × 15 min"
        ),
    )

    validate_metric_columns_exist(
        result,
        COUNTRY_15MIN_WEATHER_METRICS,
        (
            "Real Open-Meteo "
            "Province × 15 min"
        ),
    )

    validate_unique_grain(
        result,
        [
            "province_code",
            "gold_timestamp",
        ],
        (
            "Real Open-Meteo "
            "Province × 15 min"
        ),
    )

    validate_structural_columns_not_null(
        result,
        [
            "province_code",
            "gold_timestamp",
        ],
        (
            "Real Open-Meteo "
            "Province × 15 min"
        ),
    )

    validate_wind_directions(
        result,
        (
            "Real Open-Meteo "
            "Province × 15 min"
        ),
    )

    print_validation_block(
        "OPEN-METEO REAL PROVINCE-15MIN WEATHER",
        {
            "RESULT_ROWS": (
                result_count
            ),
            "DISTINCT_PROVINCES": (
                result
                .select(
                    "province_code"
                )
                .distinct()
                .count()
            ),
        },
    )


# ============================================================================
# Spain × 15 min real integration
# ============================================================================

def validate_country_15min_weather(
    open_meteo_15min: DataFrame,
) -> None:
    result = (
        prepare_country_15min_weather(
            open_meteo_15min,
            geography_key=(
                TEST_COUNTRY_GEOGRAPHY_KEY
            ),
        )
    )

    result_count = validate_non_empty(
        result,
        "Real Gold Spain × 15 min weather",
    )

    validate_metric_columns_exist(
        result,
        COUNTRY_15MIN_WEATHER_METRICS,
        "Real Gold Spain × 15 min weather",
    )

    validate_unique_grain(
        result,
        [
            "geography_key",
            "gold_timestamp",
        ],
        "Real Gold Spain × 15 min weather",
    )

    validate_structural_columns_not_null(
        result,
        [
            "geography_key",
            "gold_timestamp",
        ],
        "Real Gold Spain × 15 min weather",
    )

    validate_wind_directions(
        result,
        "Real Gold Spain × 15 min weather",
    )

    wrong_geography_count = (
        result
        .filter(
            (
                F.col(
                    "geography_key"
                )
                != F.lit(
                    TEST_COUNTRY_GEOGRAPHY_KEY
                )
            )
            |
            (
                F.col(
                    "geography_level"
                )
                != F.lit(
                    "COUNTRY"
                )
            )
            |
            (
                F.col(
                    "geography_name"
                )
                != F.lit(
                    "España"
                )
            )
        )
        .count()
    )

    if wrong_geography_count != 0:
        raise AssertionError(
            "Spain 15-minute weather contains "
            f"{wrong_geography_count} rows with "
            "unexpected geography."
        )

    print_validation_block(
        "COMPLETE REAL GOLD SPAIN-15MIN WEATHER",
        {
            "RESULT_ROWS": (
                result_count
            ),
            "GEOGRAPHY_KEY": (
                TEST_COUNTRY_GEOGRAPHY_KEY
            ),
            "MIN_TIMESTAMP": (
                result
                .agg(
                    F.min(
                        "gold_timestamp"
                    )
                )
                .first()[0]
            ),
            "MAX_TIMESTAMP": (
                result
                .agg(
                    F.max(
                        "gold_timestamp"
                    )
                )
                .first()[0]
            ),
        },
    )

def validate_peninsula_15min_weather(
    open_meteo_15min: DataFrame,
) -> None:
    """
    Validate the real Peninsula × 15 min meteorological product.

    Validated Gold geographical rule:

        Open-Meteo points
        -> Province × 15 min
        -> exclude configured non-peninsular CNIG provinces
        -> Peninsula × 15 min

    Spain is never relabelled as Peninsula.
    Peninsula is independently aggregated from real province-level weather.
    """
    excluded_codes = (
        load_peninsula_excluded_province_codes()
    )

    province = (
        prepare_open_meteo_province_15min(
            open_meteo_15min
        )
        .cache()
    )

    source_province_count = (
        province
        .select(
            "province_code"
        )
        .distinct()
        .count()
    )

    excluded_present = (
        province
        .filter(
            F.col(
                "province_code"
            ).isin(
                excluded_codes
            )
        )
        .select(
            "province_code"
        )
        .distinct()
        .count()
    )

    eligible_province_count = (
        province
        .filter(
            ~F.col(
                "province_code"
            ).isin(
                excluded_codes
            )
        )
        .select(
            "province_code"
        )
        .distinct()
        .count()
    )

    if source_province_count != 52:
        raise AssertionError(
            "Expected 52 real Open-Meteo provincial "
            "entities, found "
            f"{source_province_count}."
        )

    if excluded_present != 5:
        raise AssertionError(
            "Expected all 5 validated non-peninsular "
            "province codes in real Open-Meteo coverage, "
            f"found {excluded_present}."
        )

    if eligible_province_count != 47:
        raise AssertionError(
            "Expected 47 peninsular provincial entities, "
            f"found {eligible_province_count}."
        )

    result = (
        prepare_peninsula_15min_weather(
            open_meteo_15min,
            geography_key=(
                TEST_PENINSULA_GEOGRAPHY_KEY
            ),
            excluded_province_codes=(
                excluded_codes
            ),
        )
        .cache()
    )

    result_count = (
        result.count()
    )

    assert_required_columns(
        result,
        {
            "gold_timestamp",
            "geography_key",
            "geography_level",
            "geography_name",
            *COUNTRY_15MIN_WEATHER_METRICS,
        },
        "Real Gold Peninsula × 15 min weather",
    )

    validate_structural_columns_not_null(
        result,
        [
            "geography_key",
            "gold_timestamp",
        ],
        "Real Gold Peninsula × 15 min weather",
    )

    validate_wind_directions(
        result,
        "Real Gold Peninsula × 15 min weather",
    )

    duplicate_count = (
        result
        .groupBy(
            "geography_key",
            "gold_timestamp",
        )
        .count()
        .filter(
            F.col(
                "count"
            ) > 1
        )
        .count()
    )

    if duplicate_count != 0:
        raise AssertionError(
            "Peninsula 15-minute weather contains "
            f"{duplicate_count} duplicated Gold grains."
        )

    wrong_geography_count = (
        result
        .filter(
            (
                F.col(
                    "geography_key"
                )
                != F.lit(
                    TEST_PENINSULA_GEOGRAPHY_KEY
                )
            )
            |
            (
                F.col(
                    "geography_level"
                )
                != F.lit(
                    "PENINSULA"
                )
            )
            |
            (
                F.col(
                    "geography_name"
                )
                != F.lit(
                    "Península"
                )
            )
        )
        .count()
    )

    if wrong_geography_count != 0:
        raise AssertionError(
            "Peninsula 15-minute weather contains "
            f"{wrong_geography_count} rows with "
            "unexpected geography."
        )

    print_validation_block(
        "COMPLETE REAL GOLD PENINSULA-15MIN WEATHER",
        {
            "SOURCE_PROVINCES": (
                source_province_count
            ),
            "EXCLUDED_PROVINCES": (
                excluded_present
            ),
            "PENINSULA_PROVINCES": (
                eligible_province_count
            ),
            "RESULT_ROWS": (
                result_count
            ),
            "DUPLICATED_GRAINS": (
                duplicate_count
            ),
            "GEOGRAPHY_KEY": (
                TEST_PENINSULA_GEOGRAPHY_KEY
            ),
            "MIN_TIMESTAMP": (
                result
                .agg(
                    F.min(
                        "gold_timestamp"
                    )
                )
                .first()[0]
            ),
            "MAX_TIMESTAMP": (
                result
                .agg(
                    F.max(
                        "gold_timestamp"
                    )
                )
                .first()[0]
            ),
        },
    )

    result.unpersist()
    province.unpersist()

# ============================================================================
# Main
# ============================================================================

def main() -> None:
    spark = (
        SparkSession.builder
        .appName(
            "gold-weather-integration-validation"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 80)
    print(
        "VALIDATE GOLD WEATHER AGAINST REAL SILVER"
    )
    print("=" * 80)

    assert_table_exists(
        spark,
        TABLE_AEMET_CURRENT,
    )

    assert_table_exists(
        spark,
        TABLE_AEMET_STATIONS,
    )

    assert_table_exists(
        spark,
        TABLE_OPEN_METEO_HOURLY,
    )

    assert_table_exists(
        spark,
        TABLE_OPEN_METEO_15MIN,
    )

    (
        aemet_current,
        aemet_stations,
        open_meteo_hourly,
        open_meteo_15min,
    ) = validate_real_silver_schemas(
        spark
    )

    validate_aemet_real(
        aemet_current,
        aemet_stations,
    )

    validate_open_meteo_hourly_real(
        open_meteo_hourly
    )

    validate_open_meteo_wind_real(
        open_meteo_15min
    )

    validate_complete_province_hourly_weather(
        aemet_current,
        aemet_stations,
        open_meteo_hourly,
        open_meteo_15min,
    )

    validate_province_15min_weather(
        open_meteo_15min
    )

    validate_country_15min_weather(
        open_meteo_15min
    )

    validate_peninsula_15min_weather(
        open_meteo_15min
    )

    print("=" * 80)
    print(
        "ALL GOLD WEATHER INTEGRATION VALIDATED"
    )
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()