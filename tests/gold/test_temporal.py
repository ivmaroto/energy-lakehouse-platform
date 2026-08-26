from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from gold.common import (
    TABLE_SILVER_ESIOS_POWER_5MIN,
    TABLE_SILVER_OPEN_METEO_15MIN,
    get_esios_time_gap_hours,
    get_spark_session,
    read_silver_table,
)

from gold.temporal import (
    add_15min_timestamp,
    add_esios_5min_energy,
    add_hour_timestamp,
    aggregate_esios_energy_5min_to_15min,
    aggregate_open_meteo_wind_to_hourly_point,
    apply_esios_time_gap,
    add_deterministic_time_key,
)


# ============================================================================
# Approved ESIOS 5-minute indicators
# ============================================================================

ESIOS_5MIN_METRICS = {
    1293: "real_demand",
    2038: "wind_generation",
    2039: "nuclear_generation",
    2040: "coal_generation",
    2041: "combined_cycle_generation",
    2042: "hydraulic_generation",
    2044: "solar_photovoltaic_generation",
    2045: "solar_thermal_generation",
    2046: "renewable_thermal_generation",
    2051: "cogeneration_waste_generation",
    2065: "pumping_consumption",
}


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
        .appName("gold-temporal-unit-tests")
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


# ============================================================================
# Unit tests
# ============================================================================

def test_apply_esios_time_gap_uses_supplied_configurable_gap(
    spark,
):
    source_timestamp = datetime(
        2026,
        8,
        25,
        10,
        0,
        0,
    )

    df = spark.createDataFrame(
        [
            (
                source_timestamp,
            ),
        ],
        [
            "observation_timestamp",
        ],
    )

    result = apply_esios_time_gap(
        df,
        gap_hours=2,
    )

    row = result.first()

    assert row["gold_timestamp"] == datetime(
        2026,
        8,
        25,
        12,
        0,
        0,
    )


def test_add_hour_timestamp_truncates_to_natural_hour(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                datetime(
                    2026,
                    8,
                    25,
                    10,
                    37,
                    42,
                ),
            ),
        ],
        [
            "observation_timestamp",
        ],
    )

    result = add_hour_timestamp(
        df
    )

    row = result.first()

    assert row["gold_timestamp"] == datetime(
        2026,
        8,
        25,
        10,
        0,
        0,
    )


def test_add_15min_timestamp_assigns_natural_bucket(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                datetime(
                    2026,
                    8,
                    25,
                    10,
                    29,
                    59,
                ),
            ),
        ],
        [
            "gold_timestamp",
        ],
    )

    result = add_15min_timestamp(
        df
    )

    row = result.first()

    assert row[
        "gold_timestamp_15min"
    ] == datetime(
        2026,
        8,
        25,
        10,
        15,
        0,
    )


def test_add_esios_5min_energy_converts_mw_to_mwh_and_preserves_sign(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        25,
        10,
        0,
        0,
    )

    df = spark.createDataFrame(
        [
            (
                2038,
                120.0,
                timestamp,
            ),
            (
                2038,
                -120.0,
                timestamp,
            ),
        ],
        [
            "indicator_id",
            "value",
            "gold_timestamp",
        ],
    )

    result = (
        add_esios_5min_energy(
            df,
            metric_mapping={
                2038: "wind_generation",
            },
        )
        .orderBy(
            "value",
        )
        .collect()
    )

    assert len(result) == 2

    assert result[0]["power_mw"] == pytest.approx(
        -120.0
    )

    assert result[0][
        "energy_mwh_5min"
    ] == pytest.approx(
        -10.0
    )

    assert result[1]["power_mw"] == pytest.approx(
        120.0
    )

    assert result[1][
        "energy_mwh_5min"
    ] == pytest.approx(
        10.0
    )


def test_aggregate_esios_energy_5min_to_15min_sums_three_real_intervals(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                2038,
                1,
                "España",
                datetime(
                    2026,
                    8,
                    25,
                    10,
                    0,
                    0,
                ),
                10.0,
            ),
            (
                2038,
                1,
                "España",
                datetime(
                    2026,
                    8,
                    25,
                    10,
                    5,
                    0,
                ),
                20.0,
            ),
            (
                2038,
                1,
                "España",
                datetime(
                    2026,
                    8,
                    25,
                    10,
                    10,
                    0,
                ),
                30.0,
            ),
        ],
        [
            "indicator_id",
            "esios_geo_id",
            "esios_geo_name",
            "gold_timestamp",
            "energy_mwh_5min",
        ],
    )

    result = (
        aggregate_esios_energy_5min_to_15min(
            df
        )
    )

    assert result.count() == 1

    row = result.first()

    assert row["indicator_id"] == 2038

    assert row["gold_timestamp"] == datetime(
        2026,
        8,
        25,
        10,
        0,
        0,
    )

    assert row[
        "source_interval_count"
    ] == 3

    assert row[
        "energy_mwh_15min"
    ] == pytest.approx(
        60.0
    )


def main() -> None:
    spark = get_spark_session(
        "gold-validate-temporal-transformations"
    )

    print("=" * 80)
    print("VALIDATE GOLD TEMPORAL TRANSFORMATIONS")
    print("=" * 80)

    # ========================================================================
    # Gold configuration
    # ========================================================================

    gap_hours = get_esios_time_gap_hours()

    print(
        f"ESIOS_TIME_GAP_HOURS = {gap_hours}"
    )

    if gap_hours != 1:
        raise RuntimeError(
            "Configured ESIOS time gap does not match "
            "the currently approved value of +1 hour."
        )

    # ========================================================================
    # Open-Meteo wind: 15 min -> hour per point
    # ========================================================================

    open_meteo_15min = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_OPEN_METEO_15MIN,
    )

    open_meteo_hourly_wind = (
        aggregate_open_meteo_wind_to_hourly_point(
            open_meteo_15min
        )
    )

    om_rows = (
        open_meteo_hourly_wind
        .count()
    )

    om_bad_interval_counts = (
        open_meteo_hourly_wind
        .filter(
            F.col("source_interval_count") != 4
        )
        .count()
    )

    om_duplicate_keys = (
        open_meteo_hourly_wind
        .groupBy(
            "station_id",
            "gold_timestamp",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print("-" * 80)
    print("OPEN_METEO_15MIN_TO_HOURLY_POINT")
    print(
        f"ROWS = {om_rows}"
    )
    print(
        "ROWS_WITH_INTERVAL_COUNT_NOT_4 = "
        f"{om_bad_interval_counts}"
    )
    print(
        f"DUPLICATE_STATION_HOUR_KEYS = "
        f"{om_duplicate_keys}"
    )

    if om_bad_interval_counts != 0:
        raise RuntimeError(
            "Open-Meteo hourly wind aggregation contains "
            "hours without exactly four source intervals."
        )

    if om_duplicate_keys != 0:
        raise RuntimeError(
            "Open-Meteo hourly wind aggregation produced "
            "duplicate station/hour keys."
        )

    # ========================================================================
    # ESIOS: configurable +1h alignment
    # ========================================================================

    esios_power_5min = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_ESIOS_POWER_5MIN,
    )

    esios_aligned = apply_esios_time_gap(
        esios_power_5min,
        gap_hours=gap_hours,
    )

    gap_mismatches = (
        esios_aligned
        .filter(
            F.col("gold_timestamp")
            != (
                F.col("observation_timestamp")
                + F.expr(
                    f"INTERVAL {gap_hours} HOURS"
                )
            )
        )
        .count()
    )

    print("-" * 80)
    print("ESIOS_TEMPORAL_ALIGNMENT")
    print(
        f"GAP_MISMATCHES = {gap_mismatches}"
    )

    if gap_mismatches != 0:
        raise RuntimeError(
            "ESIOS temporal alignment does not match "
            "the configured gap."
        )

    # ========================================================================
    # ESIOS: MW -> MWh for each real 5-minute interval
    # ========================================================================

    esios_energy_5min = add_esios_5min_energy(
        esios_aligned,
        metric_mapping=ESIOS_5MIN_METRICS,
    )

    esios_5min_rows = (
        esios_energy_5min
        .count()
    )

    energy_formula_mismatches = (
        esios_energy_5min
        .filter(
            F.abs(
                F.col("energy_mwh_5min")
                - (
                    F.col("power_mw")
                    * F.lit(5.0 / 60.0)
                )
            ) > F.lit(1e-9)
        )
        .count()
    )

    sign_mismatches = (
        esios_energy_5min
        .filter(
            (
                (F.col("power_mw") < 0)
                & (F.col("energy_mwh_5min") >= 0)
            )
            |
            (
                (F.col("power_mw") > 0)
                & (F.col("energy_mwh_5min") <= 0)
            )
        )
        .count()
    )

    print("-" * 80)
    print("ESIOS_5MIN_POWER_TO_ENERGY")
    print(
        f"ROWS_SELECTED = {esios_5min_rows}"
    )
    print(
        "ENERGY_FORMULA_MISMATCHES = "
        f"{energy_formula_mismatches}"
    )
    print(
        f"SIGN_MISMATCHES = {sign_mismatches}"
    )

    if energy_formula_mismatches != 0:
        raise RuntimeError(
            "Incorrect MW -> MWh conversion detected."
        )

    if sign_mismatches != 0:
        raise RuntimeError(
            "Original ESIOS signs were not preserved."
        )

    # ========================================================================
    # ESIOS: 3 x 5 min energy -> 15 min energy
    # ========================================================================

    esios_energy_15min = (
        aggregate_esios_energy_5min_to_15min(
            esios_energy_5min
        )
    )

    esios_15min_rows = (
        esios_energy_15min
        .count()
    )

    esios_bad_interval_counts = (
        esios_energy_15min
        .filter(
            F.col("source_interval_count") != 3
        )
        .count()
    )

    esios_duplicate_keys = (
        esios_energy_15min
        .groupBy(
            "indicator_id",
            "esios_geo_id",
            "gold_timestamp",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print("-" * 80)
    print("ESIOS_5MIN_TO_15MIN")
    print(
        f"ROWS = {esios_15min_rows}"
    )
    print(
        "ROWS_WITH_INTERVAL_COUNT_NOT_3 = "
        f"{esios_bad_interval_counts}"
    )
    print(
        "DUPLICATE_INDICATOR_GEO_15MIN_KEYS = "
        f"{esios_duplicate_keys}"
    )

    if esios_bad_interval_counts != 0:
        raise RuntimeError(
            "ESIOS 15-minute aggregation contains "
            "intervals without exactly three real "
            "5-minute observations."
        )

    if esios_duplicate_keys != 0:
        raise RuntimeError(
            "ESIOS 15-minute aggregation produced "
            "duplicate indicator/geography/timestamp keys."
        )

    print("=" * 80)
    print(
        "ALL GOLD TEMPORAL TRANSFORMATIONS VALIDATED"
    )
    print("=" * 80)

    spark.stop()

def test_deterministic_time_key_is_stable_for_same_timestamp(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        25,
        18,
        15,
        0,
    )

    df = spark.createDataFrame(
        [
            (timestamp,),
            (timestamp,),
        ],
        [
            "gold_timestamp",
        ],
    )

    result = (
        add_deterministic_time_key(
            df,
            time_grain="FIFTEEN_MINUTES",
        )
        .select(
            "time_key"
        )
        .distinct()
        .collect()
    )

    assert len(result) == 1
    assert len(result[0]["time_key"]) == 64


def test_deterministic_time_key_changes_for_different_timestamps(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                datetime(
                    2026,
                    8,
                    25,
                    18,
                    0,
                    0,
                ),
            ),
            (
                datetime(
                    2026,
                    8,
                    25,
                    18,
                    15,
                    0,
                ),
            ),
        ],
        [
            "gold_timestamp",
        ],
    )

    result = (
        add_deterministic_time_key(
            df,
            time_grain="FIFTEEN_MINUTES",
        )
        .select(
            "time_key"
        )
        .distinct()
        .collect()
    )

    assert len(result) == 2


def test_deterministic_time_key_separates_different_grains(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        25,
        18,
        0,
        0,
    )

    df = spark.createDataFrame(
        [
            (timestamp,),
        ],
        [
            "gold_timestamp",
        ],
    )

    hour_key = (
        add_deterministic_time_key(
            df,
            time_grain="HOUR",
        )
        .first()["time_key"]
    )

    fifteen_minute_key = (
        add_deterministic_time_key(
            df,
            time_grain="FIFTEEN_MINUTES",
        )
        .first()["time_key"]
    )

    assert hour_key != fifteen_minute_key


if __name__ == "__main__":
    main()