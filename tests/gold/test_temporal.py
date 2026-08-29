from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from gold.common import (
    TABLE_SILVER_OPEN_METEO_15MIN,
    get_esios_time_gap_hours,
    get_spark_session,
    read_silver_table,
    TABLE_SILVER_ESIOS_ENERGY_HOURLY,
)

from gold.temporal import (
    add_hour_timestamp,
    aggregate_open_meteo_wind_to_hourly_point,
    apply_esios_time_gap,
    add_deterministic_time_key,
)


# ============================================================================
# Approved ESIOS 5-minute indicators
# ============================================================================



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








def main() -> None:
    spark = get_spark_session(
        "gold-validate-temporal-transformations"
    )

    print("=" * 80)
    print("VALIDATE GOLD TEMPORAL TRANSFORMATIONS")
    print("=" * 80)

    gap_hours = get_esios_time_gap_hours()

    print(
        f"ESIOS_TIME_GAP_HOURS = {gap_hours}"
    )

    if gap_hours != 1:
        raise RuntimeError(
            "Configured ESIOS time gap does not match "
            "the currently approved value of +1 hour."
        )

    open_meteo_15min = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_OPEN_METEO_15MIN,
    )

    open_meteo_hourly_wind = (
        aggregate_open_meteo_wind_to_hourly_point(
            open_meteo_15min
        )
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
        f"ROWS = {open_meteo_hourly_wind.count()}"
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

    esios_energy_hourly = read_silver_table(
        spark=spark,
        table_name=TABLE_SILVER_ESIOS_ENERGY_HOURLY,
    )

    esios_aligned = apply_esios_time_gap(
        esios_energy_hourly,
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
    print("ESIOS_HOURLY_TEMPORAL_ALIGNMENT")
    print(
        f"GAP_MISMATCHES = {gap_mismatches}"
    )

    if gap_mismatches != 0:
        raise RuntimeError(
            "ESIOS temporal alignment does not match "
            "the configured gap."
        )

    print("=" * 80)
    print(
        "ALL ACTIVE GOLD TEMPORAL "
        "TRANSFORMATIONS VALIDATED"
    )
    print("=" * 80)

    spark.stop()


def test_deterministic_time_key_is_stable_for_same_timestamp(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    df = spark.createDataFrame(
        [
            (timestamp,),
            (timestamp,),
        ],
        ["gold_timestamp"],
    )

    result = add_deterministic_time_key(
        df,
        time_grain="HOUR",
    )

    keys = {
        row["time_key"]
        for row in result.select("time_key").collect()
    }

    assert len(keys) == 1



def test_deterministic_time_key_changes_for_different_timestamps(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                datetime(
                    2026,
                    8,
                    24,
                    10,
                    0,
                ),
            ),
            (
                datetime(
                    2026,
                    8,
                    24,
                    11,
                    0,
                ),
            ),
        ],
        ["gold_timestamp"],
    )

    result = add_deterministic_time_key(
        df,
        time_grain="HOUR",
    )

    assert (
        result
        .select("time_key")
        .distinct()
        .count()
        == 2
    )



def test_deterministic_time_key_separates_different_grains(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    df = spark.createDataFrame(
        [
            (
                timestamp,
                "2026-08",
            ),
        ],
        [
            "gold_timestamp",
            "year_month",
        ],
    )

    hourly_key = (
        add_deterministic_time_key(
            df,
            time_grain="HOUR",
        )
        .select("time_key")
        .first()["time_key"]
    )

    monthly_key = (
        add_deterministic_time_key(
            df,
            time_grain="MONTH",
        )
        .select("time_key")
        .first()["time_key"]
    )

    assert hourly_key != monthly_key



if __name__ == "__main__":
    main()