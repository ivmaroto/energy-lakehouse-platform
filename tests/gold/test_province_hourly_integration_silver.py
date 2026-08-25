from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.metrics import (
    prepare_hourly_energy_metrics,
)

from gold.province_hourly_integration import (
    integrate_province_hourly_weather_energy,
)

from gold.temporal import (
    apply_esios_time_gap,
)

from gold.weather import (
    prepare_province_hourly_weather,
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

TABLE_ESIOS_ENERGY_HOURLY = (
    "lakehouse.silver."
    "silver_esios_energy_hourly"
)


# ============================================================================
# Gold configuration
# ============================================================================

GOLD_CONFIG_PATH = Path(
    "/opt/config/gold_config.json"
)


def load_esios_time_gap_hours() -> int:
    """
    Load the approved ESIOS temporal alignment from Gold configuration.

    The value must never be duplicated or hardcoded inside this integration
    validation.
    """
    if not GOLD_CONFIG_PATH.exists():
        raise FileNotFoundError(
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

    if "esios_time_gap_hours" not in config:
        raise ValueError(
            "Gold configuration is missing "
            "'esios_time_gap_hours'."
        )

    gap_hours = config[
        "esios_time_gap_hours"
    ]

    if (
        isinstance(
            gap_hours,
            bool,
        )
        or
        not isinstance(
            gap_hours,
            int,
        )
    ):
        raise ValueError(
            "Gold configuration "
            "'esios_time_gap_hours' "
            "must be an integer."
        )

    return gap_hours


# ============================================================================
# Grain helpers
# ============================================================================

GRAIN_COLUMNS = [
    "province_code",
    "gold_timestamp",
]


def count_duplicated_grains(
    df,
) -> int:
    """
    Count duplicated Province × hour natural keys.
    """
    return (
        df
        .groupBy(
            *GRAIN_COLUMNS
        )
        .count()
        .filter(
            F.col(
                "count"
            ) > 1
        )
        .count()
    )


def count_null_grains(
    df,
) -> int:
    """
    Count rows whose Province × hour natural key is incomplete.
    """
    return (
        df
        .filter(
            F.col(
                "province_code"
            ).isNull()
            |
            F.col(
                "gold_timestamp"
            ).isNull()
        )
        .count()
    )


# ============================================================================
# Main real integration validation
# ============================================================================

def main() -> None:
    spark = (
        SparkSession.builder
        .appName(
            "gold-province-hourly-real-integration-validation"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        "=" * 80
    )

    print(
        "VALIDATE GOLD PROVINCE-HOURLY "
        "WEATHER <-> ENERGY AGAINST REAL SILVER"
    )

    print(
        "=" * 80
    )

    # ========================================================================
    # Gold configuration
    # ========================================================================

    esios_time_gap_hours = (
        load_esios_time_gap_hours()
    )

    print(
        "ESIOS_TIME_GAP_HOURS = "
        f"{esios_time_gap_hours}"
    )

    # ========================================================================
    # Read real persisted Silver
    # ========================================================================

    aemet_current = (
        spark.table(
            TABLE_AEMET_CURRENT
        )
    )

    aemet_stations = (
        spark.table(
            TABLE_AEMET_STATIONS
        )
    )

    open_meteo_hourly = (
        spark.table(
            TABLE_OPEN_METEO_HOURLY
        )
    )

    open_meteo_15min = (
        spark.table(
            TABLE_OPEN_METEO_15MIN
        )
    )

    esios_energy_hourly = (
        spark.table(
            TABLE_ESIOS_ENERGY_HOURLY
        )
    )

    print(
        "-" * 80
    )

    print(
        "REAL SILVER SOURCES"
    )

    print(
        "AEMET_CURRENT_ROWS = "
        f"{aemet_current.count()}"
    )

    print(
        "AEMET_STATIONS_ROWS = "
        f"{aemet_stations.count()}"
    )

    print(
        "OPEN_METEO_HOURLY_ROWS = "
        f"{open_meteo_hourly.count()}"
    )

    print(
        "OPEN_METEO_15MIN_ROWS = "
        f"{open_meteo_15min.count()}"
    )

    print(
        "ESIOS_ENERGY_HOURLY_ROWS = "
        f"{esios_energy_hourly.count()}"
    )

    # ========================================================================
    # Prepare real meteorology
    #
    # Output:
    #     one row per Province × hour.
    # ========================================================================

    weather = (
        prepare_province_hourly_weather(
            aemet_current,
            aemet_stations,
            open_meteo_hourly,
            open_meteo_15min,
        )
        .cache()
    )

    weather_rows = (
        weather.count()
    )

    weather_duplicate_grains = (
        count_duplicated_grains(
            weather
        )
    )

    weather_null_grains = (
        count_null_grains(
            weather
        )
    )

    weather_distinct_provinces = (
        weather
        .select(
            "province_code"
        )
        .distinct()
        .count()
    )

    print(
        "-" * 80
    )

    print(
        "REAL GOLD WEATHER PROVINCE-HOURLY"
    )

    print(
        "WEATHER_ROWS = "
        f"{weather_rows}"
    )

    print(
        "WEATHER_DISTINCT_PROVINCES = "
        f"{weather_distinct_provinces}"
    )

    print(
        "WEATHER_DUPLICATED_GRAINS = "
        f"{weather_duplicate_grains}"
    )

    print(
        "WEATHER_NULL_GRAINS = "
        f"{weather_null_grains}"
    )

    if weather_duplicate_grains != 0:
        raise AssertionError(
            "Real Gold weather contains duplicated "
            "Province × hour grains."
        )

    if weather_null_grains != 0:
        raise AssertionError(
            "Real Gold weather contains NULL "
            "Province × hour grains."
        )

    # ========================================================================
    # Prepare real hourly energy
    #
    # Silver already contains validated canonical province geography.
    #
    # Temporal alignment is applied BEFORE metric preparation, exactly as
    # required by prepare_hourly_energy_metrics().
    # ========================================================================

    esios_temporally_aligned = (
        apply_esios_time_gap(
            esios_energy_hourly,
            gap_hours=(
                esios_time_gap_hours
            ),
        )
    )

    energy = (
        prepare_hourly_energy_metrics(
            esios_temporally_aligned
        )
        .cache()
    )

    energy_rows = (
        energy.count()
    )

    energy_duplicate_grains = (
        count_duplicated_grains(
            energy
        )
    )

    energy_null_grains = (
        count_null_grains(
            energy
        )
    )

    energy_distinct_provinces = (
        energy
        .select(
            "province_code"
        )
        .distinct()
        .count()
    )

    print(
        "-" * 80
    )

    print(
        "REAL GOLD ENERGY PROVINCE-HOURLY"
    )

    print(
        "ENERGY_ROWS = "
        f"{energy_rows}"
    )

    print(
        "ENERGY_DISTINCT_PROVINCES = "
        f"{energy_distinct_provinces}"
    )

    print(
        "ENERGY_DUPLICATED_GRAINS = "
        f"{energy_duplicate_grains}"
    )

    print(
        "ENERGY_NULL_GRAINS = "
        f"{energy_null_grains}"
    )

    if energy_duplicate_grains != 0:
        raise AssertionError(
            "Real Gold energy contains duplicated "
            "Province × hour grains."
        )

    if energy_null_grains != 0:
        raise AssertionError(
            "Real Gold energy contains NULL "
            "Province × hour grains."
        )

    # ========================================================================
    # Coverage before integration
    #
    # Both sides are already unique at Province × hour, therefore these
    # counts describe the exact cardinality expected from the approved
    # FULL OUTER integration.
    # ========================================================================

    weather_keys = (
        weather
        .select(
            *GRAIN_COLUMNS
        )
        .alias(
            "weather"
        )
    )

    energy_keys = (
        energy
        .select(
            *GRAIN_COLUMNS
        )
        .alias(
            "energy"
        )
    )

    matched_rows = (
        weather_keys
        .join(
            energy_keys,
            on=GRAIN_COLUMNS,
            how="inner",
        )
        .count()
    )

    weather_only_rows = (
        weather_keys
        .join(
            energy_keys,
            on=GRAIN_COLUMNS,
            how="left_anti",
        )
        .count()
    )

    energy_only_rows = (
        energy_keys
        .join(
            weather_keys,
            on=GRAIN_COLUMNS,
            how="left_anti",
        )
        .count()
    )

    expected_final_rows = (
        matched_rows
        +
        weather_only_rows
        +
        energy_only_rows
    )

    print(
        "-" * 80
    )

    print(
        "REAL PROVINCE-HOURLY COVERAGE BEFORE FULL OUTER"
    )

    print(
        "MATCHED_ROWS = "
        f"{matched_rows}"
    )

    print(
        "WEATHER_ONLY_ROWS = "
        f"{weather_only_rows}"
    )

    print(
        "ENERGY_ONLY_ROWS = "
        f"{energy_only_rows}"
    )

    print(
        "EXPECTED_FINAL_ROWS = "
        f"{expected_final_rows}"
    )

    # ========================================================================
    # Approved FULL OUTER integration
    # ========================================================================

    integrated = (
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )
        .cache()
    )

    final_rows = (
        integrated.count()
    )

    final_distinct_grains = (
        integrated
        .select(
            *GRAIN_COLUMNS
        )
        .distinct()
        .count()
    )

    final_duplicate_grains = (
        count_duplicated_grains(
            integrated
        )
    )

    final_null_grains = (
        count_null_grains(
            integrated
        )
    )

    final_distinct_provinces = (
        integrated
        .select(
            "province_code"
        )
        .distinct()
        .count()
    )

    timestamp_range = (
        integrated
        .agg(
            F.min(
                "gold_timestamp"
            ).alias(
                "min_timestamp"
            ),
            F.max(
                "gold_timestamp"
            ).alias(
                "max_timestamp"
            ),
        )
        .first()
    )

    print(
        "-" * 80
    )

    print(
        "REAL INTEGRATED GOLD PROVINCE-HOURLY"
    )

    print(
        "FINAL_ROWS = "
        f"{final_rows}"
    )

    print(
        "FINAL_DISTINCT_GRAINS = "
        f"{final_distinct_grains}"
    )

    print(
        "FINAL_DUPLICATED_GRAINS = "
        f"{final_duplicate_grains}"
    )

    print(
        "FINAL_NULL_GRAINS = "
        f"{final_null_grains}"
    )

    print(
        "FINAL_DISTINCT_PROVINCES = "
        f"{final_distinct_provinces}"
    )

    print(
        "MIN_TIMESTAMP = "
        f"{timestamp_range['min_timestamp']}"
    )

    print(
        "MAX_TIMESTAMP = "
        f"{timestamp_range['max_timestamp']}"
    )

    # ========================================================================
    # Final cardinality assertions
    # ========================================================================

    if final_rows != expected_final_rows:
        raise AssertionError(
            "FULL OUTER cardinality mismatch: "
            f"FINAL_ROWS={final_rows}, "
            f"EXPECTED_FINAL_ROWS={expected_final_rows}."
        )

    if final_distinct_grains != final_rows:
        raise AssertionError(
            "Integrated Province × hour result contains "
            "record multiplication."
        )

    if final_duplicate_grains != 0:
        raise AssertionError(
            "Integrated Province × hour result contains "
            "duplicated natural keys."
        )

    if final_null_grains != 0:
        raise AssertionError(
            "Integrated Province × hour result contains "
            "NULL natural keys."
        )

    # ------------------------------------------------------------------------
    # Coverage identities.
    # ------------------------------------------------------------------------

    if (
        matched_rows
        +
        weather_only_rows
    ) != weather_rows:
        raise AssertionError(
            "Weather coverage identity failed."
        )

    if (
        matched_rows
        +
        energy_only_rows
    ) != energy_rows:
        raise AssertionError(
            "Energy coverage identity failed."
        )

    print(
        "=" * 80
    )

    print(
        "ALL GOLD PROVINCE-HOURLY "
        "WEATHER <-> ENERGY INTEGRATION VALIDATED"
    )

    print(
        "=" * 80
    )

    integrated.unpersist()
    energy.unpersist()
    weather.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()