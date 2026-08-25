from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold.country_15min_integration import (
    integrate_country_15min_weather_energy,
)
from gold.geography import (
    COUNTRY_ES_GEOGRAPHY_KEY,
    PENINSULA_ES_GEOGRAPHY_KEY,
)
from gold.metrics import (
    HIGH_FREQUENCY_POWER_METRICS,
    PENINSULA_HIGH_FREQUENCY_INDICATORS,
    SPAIN_HIGH_FREQUENCY_INDICATORS,
    country_15min_energy_metric_names,
    prepare_country_15min_energy_metrics,
)
from gold.temporal import (
    add_esios_5min_energy,
    aggregate_esios_energy_5min_to_15min,
    apply_esios_time_gap,
)
from gold.weather import (
    COUNTRY_15MIN_WEATHER_METRICS,
    prepare_country_15min_weather,
    prepare_peninsula_15min_weather,
)


# ============================================================================
# Real Silver Iceberg sources
# ============================================================================

TABLE_OPEN_METEO_15MIN = (
    "lakehouse.silver."
    "silver_open_meteo_15min"
)

TABLE_ESIOS_POWER_5MIN = (
    "lakehouse.silver."
    "silver_esios_power_5min"
)


# ============================================================================
# Gold configuration
# ============================================================================

GOLD_CONFIG_PATH = Path(
    "/opt/config/gold_config.json"
)


def load_gold_config() -> dict:
    """
    Load the real Gold configuration mounted in the Spark container.
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

    return config


# ============================================================================
# Generic validation helpers
# ============================================================================

def assert_table_exists(
    spark: SparkSession,
    table_name: str,
) -> None:
    if not spark.catalog.tableExists(
        table_name
    ):
        raise AssertionError(
            "Required Silver table does not exist: "
            f"{table_name}"
        )


def validate_required_columns(
    df: DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> None:
    missing = sorted(
        required_columns
        - set(
            df.columns
        )
    )

    if missing:
        raise AssertionError(
            f"{dataset_name} missing required columns: "
            f"{missing}"
        )


def validate_unique_grain(
    df: DataFrame,
    dataset_name: str,
) -> None:
    duplicate_count = (
        df
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
            f"{dataset_name} contains "
            f"{duplicate_count} duplicated Gold grains."
        )


def validate_non_null_grain(
    df: DataFrame,
    dataset_name: str,
) -> None:
    null_count = (
        df
        .filter(
            F.col(
                "geography_key"
            ).isNull()
            |
            F.col(
                "gold_timestamp"
            ).isNull()
        )
        .count()
    )

    if null_count != 0:
        raise AssertionError(
            f"{dataset_name} contains "
            f"{null_count} NULL Gold grains."
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
# Prepare real weather
# ============================================================================

def prepare_real_weather(
    open_meteo_15min: DataFrame,
    excluded_province_codes: list[str],
) -> DataFrame:
    """
    Build the two independent real meteorological scopes:

        COUNTRY:ES
        PENINSULA:ES-PEN

    Peninsula is built from eligible provinces and is never derived by
    relabelling the Spain aggregate.
    """
    spain = (
        prepare_country_15min_weather(
            open_meteo_15min,
            geography_key=(
                COUNTRY_ES_GEOGRAPHY_KEY
            ),
        )
    )

    peninsula = (
        prepare_peninsula_15min_weather(
            open_meteo_15min,
            geography_key=(
                PENINSULA_ES_GEOGRAPHY_KEY
            ),
            excluded_province_codes=(
                excluded_province_codes
            ),
        )
    )

    result = (
        spain
        .unionByName(
            peninsula
        )
        .cache()
    )

    validate_required_columns(
        result,
        {
            "geography_key",
            "geography_level",
            "geography_name",
            "gold_timestamp",
            *COUNTRY_15MIN_WEATHER_METRICS,
        },
        "Real Gold country 15-minute weather",
    )

    validate_unique_grain(
        result,
        "Real Gold country 15-minute weather",
    )

    validate_non_null_grain(
        result,
        "Real Gold country 15-minute weather",
    )

    scope_counts = (
        result
        .groupBy(
            "geography_key",
            "geography_level",
            "geography_name",
        )
        .count()
        .orderBy(
            "geography_key"
        )
        .collect()
    )

    print_validation_block(
        "REAL GOLD COUNTRY-15MIN WEATHER",
        {
            "RESULT_ROWS": (
                result.count()
            ),
            "SCOPE_COUNTS": (
                [
                    (
                        row["geography_key"],
                        row["geography_level"],
                        row["geography_name"],
                        row["count"],
                    )
                    for row
                    in scope_counts
                ]
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

    return result


# ============================================================================
# Prepare real ESIOS energy
# ============================================================================

def prepare_real_energy(
    esios_power_5min: DataFrame,
    gap_hours: int,
) -> DataFrame:
    """
    Real Silver ESIOS pipeline:

        observation_timestamp
            -> configurable Gold temporal gap
            -> MW * 5/60
            -> MWh per real 5-minute interval
            -> SUM of three interval energies
            -> MWh per 15-minute interval
            -> assign canonical Spain/Peninsula geography
            -> pivot approved Gold metrics
    """
    aligned = (
        apply_esios_time_gap(
            esios_power_5min,
            gap_hours,
        )
    )

    energy_5min = (
        add_esios_5min_energy(
            aligned,
            HIGH_FREQUENCY_POWER_METRICS,
        )
    )

    energy_15min_long = (
        aggregate_esios_energy_5min_to_15min(
            energy_5min
        )
        .cache()
    )

    # ------------------------------------------------------------------------
    # Every valid Gold 15-minute energy interval must consist of exactly
    # three real 5-minute source intervals.
    # ------------------------------------------------------------------------

    interval_distribution = (
        energy_15min_long
        .groupBy(
            "source_interval_count"
        )
        .count()
        .orderBy(
            "source_interval_count"
        )
        .collect()
    )

    incomplete_interval_count = (
        energy_15min_long
        .filter(
            F.col(
                "source_interval_count"
            )
            != F.lit(
                3
            )
        )
        .count()
    )

    if incomplete_interval_count != 0:
        raise AssertionError(
            "Real ESIOS 15-minute aggregation contains "
            f"{incomplete_interval_count} intervals that "
            "do not contain exactly three real 5-minute observations."
        )

    scoped = (
        energy_15min_long
        .withColumn(
            "geography_key",
            F.when(
                F.col(
                    "indicator_id"
                ).isin(
                    list(
                        PENINSULA_HIGH_FREQUENCY_INDICATORS
                    )
                ),
                F.lit(
                    PENINSULA_ES_GEOGRAPHY_KEY
                ),
            )
            .when(
                F.col(
                    "indicator_id"
                ).isin(
                    list(
                        SPAIN_HIGH_FREQUENCY_INDICATORS
                    )
                ),
                F.lit(
                    COUNTRY_ES_GEOGRAPHY_KEY
                ),
            ),
        )
        .withColumn(
            "geography_level",
            F.when(
                F.col(
                    "indicator_id"
                ).isin(
                    list(
                        PENINSULA_HIGH_FREQUENCY_INDICATORS
                    )
                ),
                F.lit(
                    "PENINSULA"
                ),
            )
            .otherwise(
                F.lit(
                    "COUNTRY"
                ),
            ),
        )
        .withColumn(
            "geography_name",
            F.when(
                F.col(
                    "indicator_id"
                ).isin(
                    list(
                        PENINSULA_HIGH_FREQUENCY_INDICATORS
                    )
                ),
                F.lit(
                    "Península"
                ),
            )
            .otherwise(
                F.lit(
                    "España"
                ),
            ),
        )
    )

    invalid_scope_count = (
        scoped
        .filter(
            F.col(
                "geography_key"
            ).isNull()
            |
            F.col(
                "geography_level"
            ).isNull()
            |
            F.col(
                "geography_name"
            ).isNull()
        )
        .count()
    )

    if invalid_scope_count != 0:
        raise AssertionError(
            "Real ESIOS 15-minute energy contains "
            f"{invalid_scope_count} rows without approved "
            "Gold geographical scope."
        )

    result = (
        prepare_country_15min_energy_metrics(
            scoped
        )
        .cache()
    )

    validate_required_columns(
        result,
        {
            "geography_key",
            "geography_level",
            "geography_name",
            "gold_timestamp",
            *country_15min_energy_metric_names(),
        },
        "Real Gold country 15-minute energy",
    )

    validate_unique_grain(
        result,
        "Real Gold country 15-minute energy",
    )

    validate_non_null_grain(
        result,
        "Real Gold country 15-minute energy",
    )

    # ------------------------------------------------------------------------
    # Scope semantics:
    #
    # 1293 demand belongs only to Peninsula.
    # Selected generation metrics belong only to Spain.
    # ------------------------------------------------------------------------

    invalid_spain_demand = (
        result
        .filter(
            (
                F.col(
                    "geography_key"
                )
                ==
                F.lit(
                    COUNTRY_ES_GEOGRAPHY_KEY
                )
            )
            &
            F.col(
                "real_demand_energy_mwh_15min"
            ).isNotNull()
        )
        .count()
    )

    if invalid_spain_demand != 0:
        raise AssertionError(
            "Spain contains "
            f"{invalid_spain_demand} non-null Peninsula "
            "demand observations."
        )

    generation_metrics = [
        metric
        for metric
        in country_15min_energy_metric_names()
        if metric
        != "real_demand_energy_mwh_15min"
    ]

    generation_condition = None

    for metric in generation_metrics:
        current = F.col(
            metric
        ).isNotNull()

        if generation_condition is None:
            generation_condition = current
        else:
            generation_condition = (
                generation_condition
                |
                current
            )

    invalid_peninsula_generation = (
        result
        .filter(
            (
                F.col(
                    "geography_key"
                )
                ==
                F.lit(
                    PENINSULA_ES_GEOGRAPHY_KEY
                )
            )
            &
            generation_condition
        )
        .count()
    )

    if invalid_peninsula_generation != 0:
        raise AssertionError(
            "Peninsula contains "
            f"{invalid_peninsula_generation} non-null Spain "
            "generation observations."
        )

    scope_counts = (
        result
        .groupBy(
            "geography_key",
            "geography_level",
            "geography_name",
        )
        .count()
        .orderBy(
            "geography_key"
        )
        .collect()
    )

    print_validation_block(
        "REAL GOLD COUNTRY-15MIN ENERGY",
        {
            "ESIOS_TIME_GAP_HOURS": (
                gap_hours
            ),
            "INTERVAL_COUNT_DISTRIBUTION": (
                [
                    (
                        row["source_interval_count"],
                        row["count"],
                    )
                    for row
                    in interval_distribution
                ]
            ),
            "RESULT_ROWS": (
                result.count()
            ),
            "SCOPE_COUNTS": (
                [
                    (
                        row["geography_key"],
                        row["geography_level"],
                        row["geography_name"],
                        row["count"],
                    )
                    for row
                    in scope_counts
                ]
            ),
            "INVALID_SPAIN_DEMAND": (
                invalid_spain_demand
            ),
            "INVALID_PENINSULA_GENERATION": (
                invalid_peninsula_generation
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

    energy_15min_long.unpersist()

    return result


# ============================================================================
# Real weather <-> energy integration
# ============================================================================

def validate_real_country_15min_integration(
    weather: DataFrame,
    energy: DataFrame,
) -> None:
    """
    Validate the complete logical Gold country/Peninsula × 15 min fact against
    real Silver inputs.
    """
    weather_grains = (
        weather
        .select(
            "geography_key",
            "gold_timestamp",
        )
        .distinct()
    )

    energy_grains = (
        energy
        .select(
            "geography_key",
            "gold_timestamp",
        )
        .distinct()
    )

    matched_rows = (
        weather_grains
        .join(
            energy_grains,
            [
                "geography_key",
                "gold_timestamp",
            ],
            "inner",
        )
        .count()
    )

    weather_only_rows = (
        weather_grains
        .join(
            energy_grains,
            [
                "geography_key",
                "gold_timestamp",
            ],
            "left_anti",
        )
        .count()
    )

    energy_only_rows = (
        energy_grains
        .join(
            weather_grains,
            [
                "geography_key",
                "gold_timestamp",
            ],
            "left_anti",
        )
        .count()
    )

    expected_final_rows = (
        weather_grains
        .union(
            energy_grains
        )
        .distinct()
        .count()
    )

    result = (
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )
        .cache()
    )

    final_rows = (
        result.count()
    )

    final_distinct_grains = (
        result
        .select(
            "geography_key",
            "gold_timestamp",
        )
        .distinct()
        .count()
    )

    final_duplicate_grains = (
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

    final_null_grains = (
        result
        .filter(
            F.col(
                "geography_key"
            ).isNull()
            |
            F.col(
                "gold_timestamp"
            ).isNull()
        )
        .count()
    )

    if final_rows != expected_final_rows:
        raise AssertionError(
            "Final country 15-minute row count does not "
            "match the union of valid weather and energy grains: "
            f"expected {expected_final_rows}, found {final_rows}."
        )

    if final_rows != final_distinct_grains:
        raise AssertionError(
            "Final country 15-minute integration contains "
            "grain multiplication."
        )

    if final_duplicate_grains != 0:
        raise AssertionError(
            "Final country 15-minute integration contains "
            f"{final_duplicate_grains} duplicated grains."
        )

    if final_null_grains != 0:
        raise AssertionError(
            "Final country 15-minute integration contains "
            f"{final_null_grains} NULL grains."
        )

    scope_counts = (
        result
        .groupBy(
            "geography_key",
            "geography_level",
            "geography_name",
        )
        .count()
        .orderBy(
            "geography_key"
        )
        .collect()
    )

    print_validation_block(
        "REAL GOLD COUNTRY-15MIN WEATHER <-> ENERGY INTEGRATION",
        {
            "WEATHER_ROWS": (
                weather.count()
            ),
            "ENERGY_ROWS": (
                energy.count()
            ),
            "MATCHED_ROWS": (
                matched_rows
            ),
            "WEATHER_ONLY_ROWS": (
                weather_only_rows
            ),
            "ENERGY_ONLY_ROWS": (
                energy_only_rows
            ),
            "EXPECTED_FINAL_ROWS": (
                expected_final_rows
            ),
            "FINAL_ROWS": (
                final_rows
            ),
            "FINAL_DISTINCT_GRAINS": (
                final_distinct_grains
            ),
            "FINAL_DUPLICATED_GRAINS": (
                final_duplicate_grains
            ),
            "FINAL_NULL_GRAINS": (
                final_null_grains
            ),
            "FINAL_SCOPE_COUNTS": (
                [
                    (
                        row["geography_key"],
                        row["geography_level"],
                        row["geography_name"],
                        row["count"],
                    )
                    for row
                    in scope_counts
                ]
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


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    spark = (
        SparkSession.builder
        .appName(
            "gold-country-15min-real-silver-validation"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 80)
    print(
        "VALIDATE GOLD COUNTRY-15MIN INTEGRATION AGAINST REAL SILVER"
    )
    print("=" * 80)

    assert_table_exists(
        spark,
        TABLE_OPEN_METEO_15MIN,
    )

    assert_table_exists(
        spark,
        TABLE_ESIOS_POWER_5MIN,
    )

    config = (
        load_gold_config()
    )

    gap_hours = config.get(
        "esios_time_gap_hours"
    )

    excluded_province_codes = config.get(
        "peninsula_excluded_province_codes"
    )

    if isinstance(
        gap_hours,
        bool,
    ) or not isinstance(
        gap_hours,
        int,
    ):
        raise AssertionError(
            "Gold config 'esios_time_gap_hours' "
            "must be an integer."
        )

    if not isinstance(
        excluded_province_codes,
        list,
    ):
        raise AssertionError(
            "Gold config "
            "'peninsula_excluded_province_codes' "
            "must be a list."
        )

    open_meteo_15min = spark.table(
        TABLE_OPEN_METEO_15MIN
    )

    esios_power_5min = spark.table(
        TABLE_ESIOS_POWER_5MIN
    )

    validate_required_columns(
        open_meteo_15min,
        {
            "station_id",
            "observation_timestamp",
            "province_code",
            "province_name",
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

    validate_required_columns(
        esios_power_5min,
        {
            "indicator_id",
            "observation_timestamp",
            "esios_geo_id",
            "esios_geo_name",
            "value",
        },
        TABLE_ESIOS_POWER_5MIN,
    )

    print_validation_block(
        "REAL SILVER COUNTRY-15MIN SOURCES",
        {
            "OPEN_METEO_15MIN_ROWS": (
                open_meteo_15min.count()
            ),
            "ESIOS_POWER_5MIN_ROWS": (
                esios_power_5min.count()
            ),
            "ESIOS_TIME_GAP_HOURS": (
                gap_hours
            ),
            "PENINSULA_EXCLUDED_PROVINCE_CODES": (
                excluded_province_codes
            ),
        },
    )

    weather = prepare_real_weather(
        open_meteo_15min,
        excluded_province_codes,
    )

    energy = prepare_real_energy(
        esios_power_5min,
        gap_hours,
    )

    validate_real_country_15min_integration(
        weather,
        energy,
    )

    weather.unpersist()
    energy.unpersist()

    print("=" * 80)
    print(
        "ALL GOLD COUNTRY-15MIN REAL SILVER INTEGRATION VALIDATED"
    )
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()