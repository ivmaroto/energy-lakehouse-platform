from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

from gold.metrics import (
    COUNTRY_15MIN_WEATHER_METRICS,
    HIGH_FREQUENCY_ENERGY_15MIN_METRICS,
    HIGH_FREQUENCY_ENERGY_5MIN_METRICS,
    HIGH_FREQUENCY_EXCLUDED_INDICATORS,
    HIGH_FREQUENCY_POWER_METRICS,
    HOURLY_ENERGY_EXCLUDED_INDICATORS,
    HOURLY_ENERGY_METRICS,
    INSTALLED_CAPACITY_METRICS,
    PENINSULA_HIGH_FREQUENCY_INDICATORS,
    PROVINCE_HOURLY_WEATHER_METRICS,
    SPAIN_HIGH_FREQUENCY_INDICATORS,
    add_energy_mwh_5min,
    country_15min_energy_metric_names,
    country_5min_energy_metric_names,
    country_5min_power_metric_names,
    hourly_energy_metric_names,
    installed_capacity_metric_names,
    pivot_indicator_metrics,
    prepare_country_15min_energy_metrics,
    prepare_country_5min_metrics,
    prepare_hourly_energy_metrics,
    prepare_installed_capacity_metrics,
    select_approved_indicators,
    selected_indicator_ids,
    validate_required_columns,
    validate_unique_indicator_observations,
)


# ============================================================================
# Spark
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("gold-metrics-tests")
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

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


# ============================================================================
# Approved metric mappings
# ============================================================================

def test_hourly_energy_metric_mapping_is_exact():
    assert HOURLY_ENERGY_METRICS == {
        1159: "wind_generation_mwh",
        1161: "solar_photovoltaic_generation_mwh",
        1162: "solar_thermal_generation_mwh",
        10035: "hydraulic_generation_mwh",
        1153: "nuclear_generation_mwh",
        1156: "combined_cycle_generation_mwh",
        1158: "gas_natural_steam_turbine_generation_mwh",
        1164: "gas_natural_cogeneration_mwh",
        10036: "coal_generation_mwh",
        10041: "other_renewables_generation_mwh",
        10043: "total_generation_mwh",
    }

    assert len(
        HOURLY_ENERGY_METRICS
    ) == 11


def test_hourly_energy_exclusions_are_exact():
    assert HOURLY_ENERGY_EXCLUDED_INDICATORS == {
        10195,
        1193,
        10267,
    }


def test_installed_capacity_metric_mapping_is_exact():
    assert INSTALLED_CAPACITY_METRICS == {
        1475: "hydraulic_installed_capacity_mw",
        1485: "wind_installed_capacity_mw",
        1486: "solar_photovoltaic_installed_capacity_mw",
        1487: "solar_thermal_installed_capacity_mw",
        10302: "renewable_total_installed_capacity_mw",
        1477: "nuclear_installed_capacity_mw",
        1478: "coal_installed_capacity_mw",
        1483: "combined_cycle_installed_capacity_mw",
        1488: "other_renewables_installed_capacity_mw",
    }

    assert len(
        INSTALLED_CAPACITY_METRICS
    ) == 9


def test_high_frequency_power_metric_mapping_is_exact():
    assert HIGH_FREQUENCY_POWER_METRICS == {
        1293: "real_demand_mw",
        2038: "wind_generation_power_mw",
        2039: "nuclear_generation_power_mw",
        2040: "coal_generation_power_mw",
        2041: "combined_cycle_generation_power_mw",
        2042: "hydraulic_generation_power_mw",
        2044: "solar_photovoltaic_generation_power_mw",
        2045: "solar_thermal_generation_power_mw",
        2046: "renewable_thermal_generation_power_mw",
        2051: "cogeneration_waste_generation_power_mw",
        2065: "pumping_consumption_power_mw",
    }

    assert len(
        HIGH_FREQUENCY_POWER_METRICS
    ) == 11


def test_high_frequency_5min_energy_mapping_has_same_indicators_as_power():
    assert set(
        HIGH_FREQUENCY_ENERGY_5MIN_METRICS
    ) == set(
        HIGH_FREQUENCY_POWER_METRICS
    )

    assert len(
        HIGH_FREQUENCY_ENERGY_5MIN_METRICS
    ) == 11


def test_high_frequency_15min_energy_mapping_has_same_indicators_as_power():
    assert set(
        HIGH_FREQUENCY_ENERGY_15MIN_METRICS
    ) == set(
        HIGH_FREQUENCY_POWER_METRICS
    )

    assert len(
        HIGH_FREQUENCY_ENERGY_15MIN_METRICS
    ) == 11


def test_high_frequency_exclusion_is_exact():
    assert HIGH_FREQUENCY_EXCLUDED_INDICATORS == {
        10004,
    }


# ============================================================================
# High-frequency geographical scope
# ============================================================================

def test_peninsula_high_frequency_scope_is_demand_only():
    assert PENINSULA_HIGH_FREQUENCY_INDICATORS == {
        1293,
    }


def test_spain_high_frequency_scope_is_exact():
    assert SPAIN_HIGH_FREQUENCY_INDICATORS == {
        2038,
        2039,
        2040,
        2041,
        2042,
        2044,
        2045,
        2046,
        2051,
        2065,
    }


def test_spain_and_peninsula_high_frequency_scopes_do_not_overlap():
    assert (
        PENINSULA_HIGH_FREQUENCY_INDICATORS
        .intersection(
            SPAIN_HIGH_FREQUENCY_INDICATORS
        )
        == set()
    )


def test_all_high_frequency_indicators_have_an_approved_scope():
    scoped_indicators = (
        PENINSULA_HIGH_FREQUENCY_INDICATORS
        |
        SPAIN_HIGH_FREQUENCY_INDICATORS
    )

    assert scoped_indicators == set(
        HIGH_FREQUENCY_POWER_METRICS
    )


# ============================================================================
# Weather metric selection
# ============================================================================

def test_province_hourly_weather_metrics_are_exact():
    assert PROVINCE_HOURLY_WEATHER_METRICS == (
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed_80m",
        "wind_direction_80m",
        "wind_speed_120m",
        "wind_direction_120m",
        "solar_radiation",
        "direct_normal_irradiance",
    )


def test_country_15min_weather_metrics_are_exact():
    assert COUNTRY_15MIN_WEATHER_METRICS == (
        "temperature",
        "humidity",
        "precipitation",
        "wind_speed_80m",
        "wind_direction_80m",
        "wind_speed_120m",
        "wind_direction_120m",
        "solar_radiation",
        "direct_normal_irradiance",
    )


# ============================================================================
# Generic helpers
# ============================================================================

def test_selected_indicator_ids_are_deterministically_sorted():
    mapping = {
        30: "metric_30",
        10: "metric_10",
        20: "metric_20",
    }

    assert selected_indicator_ids(
        mapping
    ) == (
        10,
        20,
        30,
    )


def test_validate_required_columns_accepts_complete_dataframe(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                1,
                "value",
            ),
        ],
        [
            "id",
            "name",
        ],
    )

    validate_required_columns(
        df,
        {
            "id",
            "name",
        },
        "test_dataset",
    )


def test_validate_required_columns_rejects_missing_column(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                1,
            ),
        ],
        [
            "id",
        ],
    )

    with pytest.raises(
        ValueError,
        match="missing required columns",
    ):
        validate_required_columns(
            df,
            {
                "id",
                "missing_column",
            },
            "test_dataset",
        )


def test_select_approved_indicators_excludes_unapproved_rows(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                1159,
                10.0,
            ),
            (
                10043,
                20.0,
            ),
            (
                10195,
                30.0,
            ),
        ],
        [
            "indicator_id",
            "value",
        ],
    )

    result = select_approved_indicators(
        df,
        HOURLY_ENERGY_METRICS,
        dataset_name="test_dataset",
    )

    ids = {
        row["indicator_id"]
        for row in result.collect()
    }

    assert ids == {
        1159,
        10043,
    }


# ============================================================================
# Duplicate validation
# ============================================================================

def test_unique_indicator_observations_accepts_unique_rows(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "01",
                "2026-08-23 10:00:00",
                1159,
                10.0,
            ),
            (
                "01",
                "2026-08-23 10:00:00",
                10043,
                20.0,
            ),
        ],
        [
            "province_code",
            "gold_timestamp",
            "indicator_id",
            "value",
        ],
    )

    validate_unique_indicator_observations(
        df,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        indicator_column="indicator_id",
        dataset_name="test_dataset",
    )


def test_unique_indicator_observations_rejects_duplicates(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "01",
                "2026-08-23 10:00:00",
                1159,
                10.0,
            ),
            (
                "01",
                "2026-08-23 10:00:00",
                1159,
                11.0,
            ),
        ],
        [
            "province_code",
            "gold_timestamp",
            "indicator_id",
            "value",
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicated",
    ):
        validate_unique_indicator_observations(
            df,
            grain_columns=[
                "province_code",
                "gold_timestamp",
            ],
            indicator_column="indicator_id",
            dataset_name="test_dataset",
        )


# ============================================================================
# Generic long -> wide pivot
# ============================================================================

def test_pivot_indicator_metrics_creates_wide_metrics(
    spark,
):
    mapping = {
        1: "metric_a",
        2: "metric_b",
    }

    df = spark.createDataFrame(
        [
            (
                "A",
                "2026-08-23 10:00:00",
                1,
                10.0,
            ),
            (
                "A",
                "2026-08-23 10:00:00",
                2,
                20.0,
            ),
        ],
        [
            "geo",
            "gold_timestamp",
            "indicator_id",
            "value",
        ],
    )

    result = pivot_indicator_metrics(
        df,
        grain_columns=[
            "geo",
            "gold_timestamp",
        ],
        metric_mapping=mapping,
        value_column="value",
        dataset_name="test_dataset",
    )

    row = result.first()

    assert row["geo"] == "A"

    assert row["metric_a"] == pytest.approx(
        10.0
    )

    assert row["metric_b"] == pytest.approx(
        20.0
    )


def test_pivot_preserves_zero_and_missing_as_different_values(
    spark,
):
    mapping = {
        1: "real_zero",
        2: "missing_metric",
    }

    df = spark.createDataFrame(
        [
            (
                "A",
                1,
                0.0,
            ),
        ],
        [
            "geo",
            "indicator_id",
            "value",
        ],
    )

    result = pivot_indicator_metrics(
        df,
        grain_columns=[
            "geo",
        ],
        metric_mapping=mapping,
        value_column="value",
        dataset_name="test_dataset",
    )

    row = result.first()

    assert row["real_zero"] == pytest.approx(
        0.0
    )

    assert row["missing_metric"] is None


def test_pivot_preserves_negative_values(
    spark,
):
    mapping = {
        1: "signed_metric",
    }

    df = spark.createDataFrame(
        [
            (
                "A",
                1,
                -25.5,
            ),
        ],
        [
            "geo",
            "indicator_id",
            "value",
        ],
    )

    result = pivot_indicator_metrics(
        df,
        grain_columns=[
            "geo",
        ],
        metric_mapping=mapping,
        value_column="value",
        dataset_name="test_dataset",
    )

    row = result.first()

    assert row["signed_metric"] == pytest.approx(
        -25.5
    )


# ============================================================================
# Hourly ESIOS energy
# ============================================================================

def test_prepare_hourly_energy_metrics_uses_value_directly_as_mwh(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "01",
                "Araba/Álava",
                "16",
                "País Vasco/Euskadi",
                "2026-08-23 10:00:00",
                1159,
                64.562,
            ),
            (
                "01",
                "Araba/Álava",
                "16",
                "País Vasco/Euskadi",
                "2026-08-23 10:00:00",
                10043,
                2386.667,
            ),
            (
                "01",
                "Araba/Álava",
                "16",
                "País Vasco/Euskadi",
                "2026-08-23 10:00:00",
                10195,
                9999.0,
            ),
        ],
        [
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "gold_timestamp",
            "indicator_id",
            "value",
        ],
    )

    result = prepare_hourly_energy_metrics(
        df
    )

    row = result.first()

    assert row[
        "wind_generation_mwh"
    ] == pytest.approx(
        64.562
    )

    assert row[
        "total_generation_mwh"
    ] == pytest.approx(
        2386.667
    )

    assert "10195" not in result.columns


def test_hourly_total_generation_is_not_reconstructed_from_technologies(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "28",
                "Madrid",
                "13",
                "Comunidad de Madrid",
                "2026-08-23 10:00:00",
                1159,
                10.0,
            ),
            (
                "28",
                "Madrid",
                "13",
                "Comunidad de Madrid",
                "2026-08-23 10:00:00",
                1161,
                20.0,
            ),
            (
                "28",
                "Madrid",
                "13",
                "Comunidad de Madrid",
                "2026-08-23 10:00:00",
                10043,
                100.0,
            ),
        ],
        [
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "gold_timestamp",
            "indicator_id",
            "value",
        ],
    )

    row = prepare_hourly_energy_metrics(
        df
    ).first()

    assert row[
        "wind_generation_mwh"
    ] == pytest.approx(
        10.0
    )

    assert row[
        "solar_photovoltaic_generation_mwh"
    ] == pytest.approx(
        20.0
    )

    # The official ESIOS total is preserved.
    # It must not be reconstructed as 10 + 20.
    assert row[
        "total_generation_mwh"
    ] == pytest.approx(
        100.0
    )


# ============================================================================
# Monthly installed capacity
# ============================================================================

def test_prepare_installed_capacity_metrics_keeps_mw_directly(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08",
                "2026-08-01 00:00:00",
                "2026-08-01 00:00:00",
                "16",
                "País Vasco/Euskadi",
                1001,
                1485,
                1234.5,
            ),
            (
                "2026-08",
                "2026-08-01 00:00:00",
                "2026-08-01 00:00:00",
                "16",
                "País Vasco/Euskadi",
                1001,
                10302,
                2500.0,
            ),
        ],
        [
            "year_month",
            "gold_month_timestamp",
            "source_timestamp",
            "autonomous_community_code",
            "autonomous_community_name",
            "esios_geo_id",
            "indicator_id",
            "value",
        ],
    )

    row = prepare_installed_capacity_metrics(
        df
    ).first()

    assert row[
        "wind_installed_capacity_mw"
    ] == pytest.approx(
        1234.5
    )

    assert row[
        "renewable_total_installed_capacity_mw"
    ] == pytest.approx(
        2500.0
    )


def test_renewable_total_installed_capacity_is_not_reconstructed(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08",
                "2026-08-01 00:00:00",
                "2026-08-01 00:00:00",
                "01",
                "Andalucía",
                1001,
                1485,
                100.0,
            ),
            (
                "2026-08",
                "2026-08-01 00:00:00",
                "2026-08-01 00:00:00",
                "01",
                "Andalucía",
                1001,
                1486,
                200.0,
            ),
            (
                "2026-08",
                "2026-08-01 00:00:00",
                "2026-08-01 00:00:00",
                "01",
                "Andalucía",
                1001,
                10302,
                500.0,
            ),
        ],
        [
            "year_month",
            "gold_month_timestamp",
            "source_timestamp",
            "autonomous_community_code",
            "autonomous_community_name",
            "esios_geo_id",
            "indicator_id",
            "value",
        ],
    )

    row = prepare_installed_capacity_metrics(
        df
    ).first()

    assert row[
        "wind_installed_capacity_mw"
    ] == pytest.approx(
        100.0
    )

    assert row[
        "solar_photovoltaic_installed_capacity_mw"
    ] == pytest.approx(
        200.0
    )

    # Official indicator 10302 is preserved directly.
    assert row[
        "renewable_total_installed_capacity_mw"
    ] == pytest.approx(
        500.0
    )


# ============================================================================
# Five-minute MW -> interval MWh
# ============================================================================

@pytest.mark.parametrize(
    (
        "power_mw",
        "expected_energy_mwh",
    ),
    [
        (
            120.0,
            10.0,
        ),
        (
            0.0,
            0.0,
        ),
        (
            -120.0,
            -10.0,
        ),
        (
            64.562,
            64.562 / 12.0,
        ),
    ],
)
def test_add_energy_mwh_5min_uses_power_times_five_over_sixty(
    spark,
    power_mw,
    expected_energy_mwh,
):
    df = spark.createDataFrame(
        [
            (
                power_mw,
            ),
        ],
        [
            "value",
        ],
    )

    result = add_energy_mwh_5min(
        df
    )

    row = result.first()

    assert row[
        "energy_mwh_5min"
    ] == pytest.approx(
        expected_energy_mwh
    )


# ============================================================================
# Complete country 5-minute preparation
# ============================================================================

def test_prepare_country_5min_metrics_preserves_power_and_derives_energy(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "peninsula",
                "peninsula",
                "Península",
                8741,
                1293,
                120.0,
            ),
            (
                "2026-08-23 10:00:00",
                "spain",
                "country",
                "España",
                3,
                2038,
                -60.0,
            ),
        ],
        [
            "gold_timestamp",
            "geography_key",
            "geography_level",
            "geography_name",
            "esios_geo_id",
            "indicator_id",
            "value",
        ],
    )

    result = prepare_country_5min_metrics(
        df
    )

    rows = {
        row["geography_key"]: row
        for row in result.collect()
    }

    peninsula = rows[
        "peninsula"
    ]

    assert peninsula[
        "real_demand_mw"
    ] == pytest.approx(
        120.0
    )

    assert peninsula[
        "real_demand_energy_mwh_5min"
    ] == pytest.approx(
        10.0
    )

    spain = rows[
        "spain"
    ]

    assert spain[
        "wind_generation_power_mw"
    ] == pytest.approx(
        -60.0
    )

    assert spain[
        "wind_generation_energy_mwh_5min"
    ] == pytest.approx(
        -5.0
    )


def test_prepare_country_5min_metrics_excludes_indicator_10004(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "spain",
                "country",
                "España",
                3,
                2038,
                100.0,
            ),
            (
                "2026-08-23 10:00:00",
                "spain",
                "country",
                "España",
                3,
                10004,
                9999.0,
            ),
        ],
        [
            "gold_timestamp",
            "geography_key",
            "geography_level",
            "geography_name",
            "esios_geo_id",
            "indicator_id",
            "value",
        ],
    )

    result = prepare_country_5min_metrics(
        df
    )

    assert result.count() == 1

    assert "10004" not in result.columns


# ============================================================================
# Fifteen-minute energy
# ============================================================================

def test_prepare_country_15min_energy_metrics_uses_precalculated_energy(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "spain",
                "country",
                "España",
                2038,
                15.0,
            ),
            (
                "2026-08-23 10:00:00",
                "spain",
                "country",
                "España",
                2041,
                30.0,
            ),
        ],
        [
            "gold_timestamp",
            "geography_key",
            "geography_level",
            "geography_name",
            "indicator_id",
            "energy_mwh_15min",
        ],
    )

    row = (
        prepare_country_15min_energy_metrics(
            df
        )
        .first()
    )

    assert row[
        "wind_generation_energy_mwh_15min"
    ] == pytest.approx(
        15.0
    )

    assert row[
        "combined_cycle_generation_energy_mwh_15min"
    ] == pytest.approx(
        30.0
    )


def test_prepare_country_15min_energy_metrics_does_not_require_power_column(
    spark,
):
    """
    metrics.py must pivot an already-computed 15-minute energy.

    The 5 -> 15 minute aggregation belongs to temporal.py.
    This transformation must therefore not depend on a power_mw column
    or attempt SUM(power_mw).
    """
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "spain",
                "country",
                "España",
                2038,
                25.0,
            ),
        ],
        [
            "gold_timestamp",
            "geography_key",
            "geography_level",
            "geography_name",
            "indicator_id",
            "energy_mwh_15min",
        ],
    )

    result = prepare_country_15min_energy_metrics(
        df
    )

    row = result.first()

    assert row[
        "wind_generation_energy_mwh_15min"
    ] == pytest.approx(
        25.0
    )


# ============================================================================
# Public metric-name helpers
# ============================================================================

def test_hourly_energy_metric_names_match_mapping():
    assert hourly_energy_metric_names() == tuple(
        HOURLY_ENERGY_METRICS.values()
    )


def test_installed_capacity_metric_names_match_mapping():
    assert installed_capacity_metric_names() == tuple(
        INSTALLED_CAPACITY_METRICS.values()
    )


def test_country_5min_power_metric_names_match_mapping():
    assert country_5min_power_metric_names() == tuple(
        HIGH_FREQUENCY_POWER_METRICS.values()
    )


def test_country_5min_energy_metric_names_match_mapping():
    assert country_5min_energy_metric_names() == tuple(
        HIGH_FREQUENCY_ENERGY_5MIN_METRICS.values()
    )


def test_country_15min_energy_metric_names_match_mapping():
    assert country_15min_energy_metric_names() == tuple(
        HIGH_FREQUENCY_ENERGY_15MIN_METRICS.values()
    )