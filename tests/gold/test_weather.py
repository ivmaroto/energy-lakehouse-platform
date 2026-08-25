from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gold.weather import (
    COUNTRY_15MIN_WEATHER_METRICS,
    PROVINCE_HOURLY_WEATHER_METRICS,
    circular_mean_expression,
    prepare_aemet_province_hourly,
    prepare_country_15min_weather,
    prepare_open_meteo_province_15min,
    prepare_open_meteo_province_hourly,
    prepare_open_meteo_wind_point_hourly,
    prepare_open_meteo_wind_province_hourly,
    prepare_peninsula_15min_weather,
    prepare_province_hourly_weather,
    validate_non_null_structural_columns,
    validate_required_columns,
    validate_unique_grain,
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
        .appName("gold-weather-tests")
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
# Metric contracts
# ============================================================================

def test_province_hourly_weather_metric_contract_is_exact():
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


def test_country_15min_weather_metric_contract_is_exact():
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
# Generic validators
# ============================================================================

def test_validate_required_columns_accepts_complete_dataframe(
    spark,
):
    df = spark.createDataFrame(
        [
            (1, "A"),
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
            (1,),
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


def test_validate_unique_grain_accepts_unique_rows(
    spark,
):
    df = spark.createDataFrame(
        [
            ("01", "2026-08-23 10:00:00"),
            ("02", "2026-08-23 10:00:00"),
        ],
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    validate_unique_grain(
        df,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name="test_dataset",
    )


def test_validate_unique_grain_rejects_duplicates(
    spark,
):
    df = spark.createDataFrame(
        [
            ("01", "2026-08-23 10:00:00"),
            ("01", "2026-08-23 10:00:00"),
        ],
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicated Gold grains",
    ):
        validate_unique_grain(
            df,
            grain_columns=[
                "province_code",
                "gold_timestamp",
            ],
            dataset_name="test_dataset",
        )


def test_validate_non_null_structural_columns_rejects_nulls(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                None,
                "2026-08-23 10:00:00",
            ),
        ],
        (
            "province_code string, "
            "gold_timestamp string"
        ),
    )

    with pytest.raises(
        ValueError,
        match="NULL structural columns",
    ):
        validate_non_null_structural_columns(
            df,
            structural_columns=[
                "province_code",
                "gold_timestamp",
            ],
            dataset_name="test_dataset",
        )


# ============================================================================
# Circular mean
# ============================================================================

@pytest.mark.parametrize(
    (
        "directions",
        "expected",
    ),
    [
        (
            [90.0, 90.0],
            90.0,
        ),
        (
            [180.0, 180.0],
            180.0,
        ),
        (
            [350.0, 10.0],
            0.0,
        ),
        (
            [10.0, 350.0],
            0.0,
        ),
    ],
)
def test_circular_mean_expression(
    spark,
    directions,
    expected,
):
    df = spark.createDataFrame(
        [
            (direction,)
            for direction in directions
        ],
        [
            "direction",
        ],
    )

    row = (
        df
        .agg(
            circular_mean_expression(
                "direction"
            ).alias(
                "mean_direction"
            )
        )
        .first()
    )

    assert row[
        "mean_direction"
    ] == pytest.approx(
        expected,
        abs=1e-6,
    )


def test_circular_mean_ignores_nulls(
    spark,
):
    df = spark.createDataFrame(
        [
            (350.0,),
            (10.0,),
            (None,),
        ],
        [
            "direction",
        ],
    )

    row = (
        df
        .agg(
            circular_mean_expression(
                "direction"
            ).alias(
                "mean_direction"
            )
        )
        .first()
    )

    assert row[
        "mean_direction"
    ] == pytest.approx(
        0.0,
        abs=1e-6,
    )


# ============================================================================
# AEMET Province × hour
# ============================================================================

def test_prepare_aemet_province_hourly_aggregates_stations(
    spark,
):
    observations = spark.createDataFrame(
        [
            (
                "A",
                "2026-08-23 10:00:00",
                20.0,
                50.0,
                1.0,
            ),
            (
                "B",
                "2026-08-23 10:00:00",
                24.0,
                70.0,
                3.0,
            ),
        ],
        [
            "station_id",
            "observation_timestamp",
            "ta",
            "hr",
            "prec",
        ],
    )

    stations = spark.createDataFrame(
        [
            (
                "A",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
            ),
            (
                "B",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
            ),
        ],
        [
            "station_id",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    row = (
        prepare_aemet_province_hourly(
            observations,
            stations,
        )
        .first()
    )

    assert row[
        "aemet_temperature"
    ] == pytest.approx(
        22.0
    )

    assert row[
        "aemet_humidity"
    ] == pytest.approx(
        60.0
    )

    assert row[
        "aemet_precipitation"
    ] == pytest.approx(
        2.0
    )


def test_prepare_aemet_province_hourly_excludes_unmatched_station(
    spark,
):
    observations = spark.createDataFrame(
        [
            (
                "A",
                "2026-08-23 10:00:00",
                20.0,
                50.0,
                1.0,
            ),
            (
                "UNKNOWN",
                "2026-08-23 10:00:00",
                30.0,
                80.0,
                5.0,
            ),
        ],
        [
            "station_id",
            "observation_timestamp",
            "ta",
            "hr",
            "prec",
        ],
    )

    stations = spark.createDataFrame(
        [
            (
                "A",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
            ),
        ],
        [
            "station_id",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    result = (
        prepare_aemet_province_hourly(
            observations,
            stations,
        )
    )

    assert result.count() == 1

    row = result.first()

    assert row[
        "province_code"
    ] == "01"

    assert row[
        "aemet_temperature"
    ] == pytest.approx(
        20.0
    )

    assert row[
        "aemet_humidity"
    ] == pytest.approx(
        50.0
    )

    assert row[
        "aemet_precipitation"
    ] == pytest.approx(
        1.0
    )


# ============================================================================
# Open-Meteo hourly Province × hour
# ============================================================================

def test_prepare_open_meteo_province_hourly_aggregates_points(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                20.0,
                50.0,
                1.0,
                100.0,
                200.0,
            ),
            (
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                24.0,
                70.0,
                3.0,
                300.0,
                400.0,
            ),
        ],
        [
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
        ],
    )

    row = (
        prepare_open_meteo_province_hourly(
            df
        )
        .first()
    )

    assert row[
        "open_meteo_temperature"
    ] == pytest.approx(
        22.0
    )

    assert row[
        "open_meteo_humidity"
    ] == pytest.approx(
        60.0
    )

    assert row[
        "open_meteo_precipitation"
    ] == pytest.approx(
        2.0
    )

    assert row[
        "solar_radiation"
    ] == pytest.approx(
        200.0
    )

    assert row[
        "direct_normal_irradiance"
    ] == pytest.approx(
        300.0
    )


# ============================================================================
# Open-Meteo wind 15 min -> hour
# ============================================================================

def test_prepare_open_meteo_wind_point_hourly_uses_avg_and_circular_mean(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "station-1",
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                10.0,
                350.0,
                20.0,
                350.0,
            ),
            (
                "station-1",
                "2026-08-23 10:15:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                20.0,
                10.0,
                40.0,
                10.0,
            ),
            (
                "station-1",
                "2026-08-23 10:30:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                30.0,
                350.0,
                60.0,
                350.0,
            ),
            (
                "station-1",
                "2026-08-23 10:45:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                40.0,
                10.0,
                80.0,
                10.0,
            ),
        ],
        [
            "station_id",
            "observation_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "wind_speed_80m",
            "wind_direction_80m",
            "wind_speed_120m",
            "wind_direction_120m",
        ],
    )

    row = (
        prepare_open_meteo_wind_point_hourly(
            df
        )
        .first()
    )

    assert row[
        "wind_speed_80m"
    ] == pytest.approx(
        25.0
    )

    assert row[
        "wind_speed_120m"
    ] == pytest.approx(
        50.0
    )

    assert row[
        "wind_direction_80m"
    ] == pytest.approx(
        0.0,
        abs=1e-6,
    )

    assert row[
        "wind_direction_120m"
    ] == pytest.approx(
        0.0,
        abs=1e-6,
    )


# ============================================================================
# Open-Meteo wind Province × hour
# ============================================================================

def test_prepare_open_meteo_wind_province_hourly_aggregates_points(
    spark,
):
    rows = []

    for station_id, speed_80, speed_120, direction in [
        (
            "station-1",
            10.0,
            20.0,
            350.0,
        ),
        (
            "station-2",
            30.0,
            60.0,
            10.0,
        ),
    ]:
        for minute in [
            "00",
            "15",
            "30",
            "45",
        ]:
            rows.append(
                (
                    station_id,
                    f"2026-08-23 10:{minute}:00",
                    "01",
                    "Álava",
                    "16",
                    "País Vasco/Euskadi",
                    speed_80,
                    direction,
                    speed_120,
                    direction,
                )
            )

    df = spark.createDataFrame(
        rows,
        [
            "station_id",
            "observation_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "wind_speed_80m",
            "wind_direction_80m",
            "wind_speed_120m",
            "wind_direction_120m",
        ],
    )

    row = (
        prepare_open_meteo_wind_province_hourly(
            df
        )
        .first()
    )

    assert row[
        "wind_speed_80m"
    ] == pytest.approx(
        20.0
    )

    assert row[
        "wind_speed_120m"
    ] == pytest.approx(
        40.0
    )

    assert row[
        "wind_direction_80m"
    ] == pytest.approx(
        0.0,
        abs=1e-6,
    )


# ============================================================================
# Province-hourly weather fallback
# ============================================================================

def test_prepare_province_hourly_weather_uses_aemet_priority_per_variable(
    spark,
):
    aemet_current = spark.createDataFrame(
        [
            (
                "A",
                "2026-08-23 10:00:00",
                20.0,
                None,
                1.0,
            ),
        ],
        (
            "station_id string, "
            "observation_timestamp string, "
            "ta double, "
            "hr double, "
            "prec double"
        ),
    )

    aemet_stations = spark.createDataFrame(
        [
            (
                "A",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
            ),
        ],
        [
            "station_id",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    open_meteo_hourly = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                25.0,
                70.0,
                5.0,
                300.0,
                400.0,
            ),
        ],
        [
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
        ],
    )

    open_meteo_15min = spark.createDataFrame(
        [
            (
                "A",
                f"2026-08-23 10:{minute}:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                10.0,
                90.0,
                20.0,
                180.0,
            )
            for minute in [
                "00",
                "15",
                "30",
                "45",
            ]
        ],
        [
            "station_id",
            "observation_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "wind_speed_80m",
            "wind_direction_80m",
            "wind_speed_120m",
            "wind_direction_120m",
        ],
    )

    row = (
        prepare_province_hourly_weather(
            aemet_current,
            aemet_stations,
            open_meteo_hourly,
            open_meteo_15min,
        )
        .first()
    )

    # AEMET available -> AEMET wins.
    assert row[
        "temperature"
    ] == pytest.approx(
        20.0
    )

    assert row[
        "temperature_source"
    ] == "AEMET"

    # AEMET humidity NULL -> fallback to Open-Meteo.
    assert row[
        "humidity"
    ] == pytest.approx(
        70.0
    )

    assert row[
        "humidity_source"
    ] == "OPEN_METEO"

    # AEMET precipitation available -> AEMET wins.
    assert row[
        "precipitation"
    ] == pytest.approx(
        1.0
    )

    assert row[
        "precipitation_source"
    ] == "AEMET"

    assert row[
        "wind_speed_80m"
    ] == pytest.approx(
        10.0
    )

    assert row[
        "wind_speed_120m"
    ] == pytest.approx(
        20.0
    )

    assert row[
        "solar_radiation"
    ] == pytest.approx(
        300.0
    )

    assert row[
        "direct_normal_irradiance"
    ] == pytest.approx(
        400.0
    )


def test_prepare_province_hourly_weather_does_not_convert_missing_to_zero(
    spark,
):
    aemet_current = spark.createDataFrame(
        [],
        (
            "station_id string, "
            "observation_timestamp string, "
            "ta double, "
            "hr double, "
            "prec double"
        ),
    )

    aemet_stations = spark.createDataFrame(
        [],
        (
            "station_id string, "
            "province_code string, "
            "province_name string, "
            "autonomous_community_code string, "
            "autonomous_community_name string"
        ),
    )

    open_meteo_hourly = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                None,
                None,
                None,
                None,
                None,
            ),
        ],
        (
            "observation_timestamp string, "
            "province_code string, "
            "province_name string, "
            "autonomous_community_code string, "
            "autonomous_community_name string, "
            "temperature_2m double, "
            "relative_humidity_2m double, "
            "precipitation double, "
            "shortwave_radiation double, "
            "direct_normal_irradiance double"
        ),
    )

    open_meteo_15min = spark.createDataFrame(
        [
            (
                "A",
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                "16",
                "País Vasco/Euskadi",
                None,
                None,
                None,
                None,
            ),
        ],
        (
            "station_id string, "
            "observation_timestamp string, "
            "province_code string, "
            "province_name string, "
            "autonomous_community_code string, "
            "autonomous_community_name string, "
            "wind_speed_80m double, "
            "wind_direction_80m double, "
            "wind_speed_120m double, "
            "wind_direction_120m double"
        ),
    )

    row = (
        prepare_province_hourly_weather(
            aemet_current,
            aemet_stations,
            open_meteo_hourly,
            open_meteo_15min,
        )
        .first()
    )

    assert row["temperature"] is None
    assert row["humidity"] is None
    assert row["precipitation"] is None
    assert row["wind_speed_80m"] is None
    assert row["solar_radiation"] is None


# ============================================================================
# Province × 15 min
# ============================================================================

def test_prepare_open_meteo_province_15min_aggregates_points(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                20.0,
                50.0,
                1.0,
                10.0,
                350.0,
                20.0,
                350.0,
                100.0,
                200.0,
            ),
            (
                "2026-08-23 10:00:00",
                "01",
                "Álava",
                24.0,
                70.0,
                3.0,
                30.0,
                10.0,
                40.0,
                10.0,
                300.0,
                400.0,
            ),
        ],
        [
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
        ],
    )

    row = (
        prepare_open_meteo_province_15min(
            df
        )
        .first()
    )

    assert row[
        "temperature"
    ] == pytest.approx(
        22.0
    )

    assert row[
        "humidity"
    ] == pytest.approx(
        60.0
    )

    assert row[
        "precipitation"
    ] == pytest.approx(
        2.0
    )

    assert row[
        "wind_speed_80m"
    ] == pytest.approx(
        20.0
    )

    assert row[
        "wind_direction_80m"
    ] == pytest.approx(
        0.0,
        abs=1e-6,
    )

    assert row[
        "solar_radiation"
    ] == pytest.approx(
        200.0
    )


# ============================================================================
# Country × 15 min
# ============================================================================

def test_prepare_country_15min_weather_uses_avg_of_provinces(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "01",
                "Province A",
                20.0,
                40.0,
                1.0,
                10.0,
                350.0,
                20.0,
                350.0,
                100.0,
                200.0,
            ),
            (
                "2026-08-23 10:00:00",
                "02",
                "Province B",
                30.0,
                60.0,
                3.0,
                30.0,
                10.0,
                40.0,
                10.0,
                300.0,
                400.0,
            ),
        ],
        [
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
        ],
    )

    row = (
        prepare_country_15min_weather(
            df,
            geography_key="COUNTRY_ES",
        )
        .first()
    )

    assert row[
        "geography_key"
    ] == "COUNTRY_ES"

    assert row[
        "geography_level"
    ] == "COUNTRY"

    assert row[
        "geography_name"
    ] == "España"

    assert row[
        "temperature"
    ] == pytest.approx(
        25.0
    )

    assert row[
        "humidity"
    ] == pytest.approx(
        50.0
    )

    assert row[
        "precipitation"
    ] == pytest.approx(
        2.0
    )

    assert row[
        "wind_speed_80m"
    ] == pytest.approx(
        20.0
    )

    assert row[
        "wind_direction_80m"
    ] == pytest.approx(
        0.0,
        abs=1e-6,
    )

    assert row[
        "solar_radiation"
    ] == pytest.approx(
        200.0
    )

    assert row[
        "direct_normal_irradiance"
    ] == pytest.approx(
        300.0
    )


def test_prepare_country_15min_weather_does_not_create_peninsula_geography(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "2026-08-23 10:00:00",
                "01",
                "Province A",
                20.0,
                40.0,
                1.0,
                10.0,
                90.0,
                20.0,
                180.0,
                100.0,
                200.0,
            ),
        ],
        [
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
        ],
    )

    row = (
        prepare_country_15min_weather(
            df,
            geography_key="COUNTRY_ES",
        )
        .first()
    )

    assert (
        row["geography_level"]
        == "COUNTRY"
    )

    assert (
        row["geography_name"]
        == "España"
    )


def test_prepare_peninsula_15min_weather_excludes_non_peninsular_provinces(
    spark,
):
    """
    Peninsula weather must be aggregated from province-level observations
    after excluding the five CNIG-validated non-peninsular province codes.

    Validated exclusions:
        07 -> Illes Balears
        35 -> Las Palmas
        38 -> Santa Cruz de Tenerife
        51 -> Ceuta
        52 -> Melilla
    """
    schema = StructType(
        [
            StructField(
                "observation_timestamp",
                TimestampType(),
                False,
            ),
            StructField(
                "province_code",
                StringType(),
                False,
            ),
            StructField(
                "province_name",
                StringType(),
                False,
            ),
            StructField(
                "temperature_2m",
                DoubleType(),
                True,
            ),
            StructField(
                "relative_humidity_2m",
                DoubleType(),
                True,
            ),
            StructField(
                "precipitation",
                DoubleType(),
                True,
            ),
            StructField(
                "wind_speed_80m",
                DoubleType(),
                True,
            ),
            StructField(
                "wind_direction_80m",
                DoubleType(),
                True,
            ),
            StructField(
                "wind_speed_120m",
                DoubleType(),
                True,
            ),
            StructField(
                "wind_direction_120m",
                DoubleType(),
                True,
            ),
            StructField(
                "shortwave_radiation",
                DoubleType(),
                True,
            ),
            StructField(
                "direct_normal_irradiance",
                DoubleType(),
                True,
            ),
        ]
    )

    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    rows = [
        (
            timestamp,
            "28",
            "Madrid",
            20.0,
            50.0,
            0.0,
            10.0,
            180.0,
            12.0,
            190.0,
            300.0,
            200.0,
        ),
        (
            timestamp,
            "07",
            "Illes Balears",
            100.0,
            90.0,
            10.0,
            50.0,
            90.0,
            60.0,
            90.0,
            900.0,
            800.0,
        ),
        (
            timestamp,
            "35",
            "Las Palmas",
            100.0,
            90.0,
            10.0,
            50.0,
            90.0,
            60.0,
            90.0,
            900.0,
            800.0,
        ),
        (
            timestamp,
            "38",
            "Santa Cruz de Tenerife",
            100.0,
            90.0,
            10.0,
            50.0,
            90.0,
            60.0,
            90.0,
            900.0,
            800.0,
        ),
        (
            timestamp,
            "51",
            "Ceuta",
            100.0,
            90.0,
            10.0,
            50.0,
            90.0,
            60.0,
            90.0,
            900.0,
            800.0,
        ),
        (
            timestamp,
            "52",
            "Melilla",
            100.0,
            90.0,
            10.0,
            50.0,
            90.0,
            60.0,
            90.0,
            900.0,
            800.0,
        ),
    ]

    source = spark.createDataFrame(
        rows,
        schema=schema,
    )

    result = (
        prepare_peninsula_15min_weather(
            source,
            geography_key="TEST_PENINSULA",
            excluded_province_codes=[
                "07",
                "35",
                "38",
                "51",
                "52",
            ],
        )
    )

    assert result.count() == 1

    row = result.first()

    assert row[
        "geography_key"
    ] == "TEST_PENINSULA"

    assert row[
        "geography_level"
    ] == "PENINSULA"

    assert row[
        "geography_name"
    ] == "Península"

    # Only Madrid is eligible for the Peninsula aggregation.
    # If any excluded territory entered the calculation this value
    # would differ dramatically.
    assert row[
        "temperature"
    ] == pytest.approx(
        20.0
    )

    assert row[
        "humidity"
    ] == pytest.approx(
        50.0
    )

    assert row[
        "precipitation"
    ] == pytest.approx(
        0.0
    )

    assert row[
        "solar_radiation"
    ] == pytest.approx(
        300.0
    )

    assert row[
        "direct_normal_irradiance"
    ] == pytest.approx(
        200.0
    )