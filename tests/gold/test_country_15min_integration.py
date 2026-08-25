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

from gold.country_15min_integration import (
    integrate_country_15min_weather_energy,
)


# ============================================================================
# Spark fixture
# ============================================================================

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName(
            "test-gold-country-15min-integration"
        )
        .config(
            "spark.ui.enabled",
            "false",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "1",
        )
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

    session.sparkContext.setLogLevel(
        "ERROR"
    )

    yield session

    session.stop()


# ============================================================================
# Schemas
# ============================================================================

WEATHER_SCHEMA = StructType(
    [
        StructField(
            "geography_key",
            StringType(),
            False,
        ),
        StructField(
            "gold_timestamp",
            TimestampType(),
            False,
        ),
        StructField(
            "geography_level",
            StringType(),
            False,
        ),
        StructField(
            "geography_name",
            StringType(),
            False,
        ),
        StructField(
            "temperature",
            DoubleType(),
            True,
        ),
        StructField(
            "humidity",
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
            "solar_radiation",
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


ENERGY_SCHEMA = StructType(
    [
        StructField(
            "geography_key",
            StringType(),
            False,
        ),
        StructField(
            "gold_timestamp",
            TimestampType(),
            False,
        ),
        StructField(
            "geography_level",
            StringType(),
            False,
        ),
        StructField(
            "geography_name",
            StringType(),
            False,
        ),
        StructField(
            "real_demand_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "wind_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "nuclear_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "coal_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "combined_cycle_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "hydraulic_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "solar_photovoltaic_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "solar_thermal_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "renewable_thermal_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "cogeneration_waste_generation_energy_mwh_15min",
            DoubleType(),
            True,
        ),
        StructField(
            "pumping_consumption_energy_mwh_15min",
            DoubleType(),
            True,
        ),
    ]
)


# ============================================================================
# Row builders
# ============================================================================

def weather_row(
    *,
    geography_key: str,
    timestamp: datetime,
    geography_level: str,
    geography_name: str,
    temperature: float = 20.0,
) -> tuple:
    return (
        geography_key,
        timestamp,
        geography_level,
        geography_name,
        temperature,
        50.0,
        0.0,
        10.0,
        180.0,
        12.0,
        190.0,
        300.0,
        200.0,
    )


def energy_row(
    *,
    geography_key: str,
    timestamp: datetime,
    geography_level: str,
    geography_name: str,
    demand: float | None = None,
    wind: float | None = None,
    nuclear: float | None = None,
    coal: float | None = None,
    combined_cycle: float | None = None,
    hydraulic: float | None = None,
    solar_photovoltaic: float | None = None,
    solar_thermal: float | None = None,
    renewable_thermal: float | None = None,
    cogeneration_waste: float | None = None,
    pumping: float | None = None,
) -> tuple:
    return (
        geography_key,
        timestamp,
        geography_level,
        geography_name,
        demand,
        wind,
        nuclear,
        coal,
        combined_cycle,
        hydraulic,
        solar_photovoltaic,
        solar_thermal,
        renewable_thermal,
        cogeneration_waste,
        pumping,
    )


# ============================================================================
# Tests
# ============================================================================

def test_matching_spain_grain_produces_one_integrated_row(
    spark: SparkSession,
) -> None:
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp,
                geography_level="COUNTRY",
                geography_name="España",
                temperature=25.0,
            )
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp,
                geography_level="COUNTRY",
                geography_name="España",
                wind=100.0,
                nuclear=150.0,
            )
        ],
        ENERGY_SCHEMA,
    )

    result = (
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )
    )

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["geography_key"] == "COUNTRY_ES"
    assert rows[0]["geography_level"] == "COUNTRY"
    assert rows[0]["geography_name"] == "España"
    assert rows[0]["temperature"] == pytest.approx(25.0)
    assert (
        rows[0]["wind_generation_energy_mwh_15min"]
        == pytest.approx(100.0)
    )
    assert (
        rows[0]["nuclear_generation_energy_mwh_15min"]
        == pytest.approx(150.0)
    )


def test_matching_peninsula_grain_preserves_demand(
    spark: SparkSession,
) -> None:
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        15,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="PENINSULA_ES",
                timestamp=timestamp,
                geography_level="PENINSULA",
                geography_name="Península",
                temperature=22.0,
            )
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="PENINSULA_ES",
                timestamp=timestamp,
                geography_level="PENINSULA",
                geography_name="Península",
                demand=500.0,
            )
        ],
        ENERGY_SCHEMA,
    )

    result = (
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )
    )

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["geography_level"] == "PENINSULA"
    assert (
        rows[0]["real_demand_energy_mwh_15min"]
        == pytest.approx(500.0)
    )
    assert (
        rows[0]["wind_generation_energy_mwh_15min"]
        is None
    )


def test_weather_only_grain_is_preserved(
    spark: SparkSession,
) -> None:
    timestamp_weather = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    timestamp_energy = datetime(
        2026,
        8,
        23,
        10,
        15,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_weather,
                geography_level="COUNTRY",
                geography_name="España",
            )
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_energy,
                geography_level="COUNTRY",
                geography_name="España",
                wind=100.0,
            )
        ],
        ENERGY_SCHEMA,
    )

    result = (
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )
    )

    weather_only = (
        result
        .filter(
            result.gold_timestamp
            == timestamp_weather
        )
        .collect()
    )

    assert len(weather_only) == 1
    assert weather_only[0]["temperature"] is not None
    assert (
        weather_only[0]["wind_generation_energy_mwh_15min"]
        is None
    )


def test_energy_only_grain_is_preserved(
    spark: SparkSession,
) -> None:
    timestamp_weather = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    timestamp_energy = datetime(
        2026,
        8,
        23,
        10,
        15,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_weather,
                geography_level="COUNTRY",
                geography_name="España",
            )
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_energy,
                geography_level="COUNTRY",
                geography_name="España",
                wind=100.0,
            )
        ],
        ENERGY_SCHEMA,
    )

    result = (
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )
    )

    energy_only = (
        result
        .filter(
            result.gold_timestamp
            == timestamp_energy
        )
        .collect()
    )

    assert len(energy_only) == 1
    assert energy_only[0]["temperature"] is None
    assert (
        energy_only[0]["wind_generation_energy_mwh_15min"]
        == pytest.approx(100.0)
    )


def test_final_grain_is_union_of_weather_and_energy(
    spark: SparkSession,
) -> None:
    timestamp_1 = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    timestamp_2 = datetime(
        2026,
        8,
        23,
        10,
        15,
    )

    timestamp_3 = datetime(
        2026,
        8,
        23,
        10,
        30,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_1,
                geography_level="COUNTRY",
                geography_name="España",
            ),
            weather_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_2,
                geography_level="COUNTRY",
                geography_name="España",
            ),
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_2,
                geography_level="COUNTRY",
                geography_name="España",
                wind=100.0,
            ),
            energy_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp_3,
                geography_level="COUNTRY",
                geography_name="España",
                wind=110.0,
            ),
        ],
        ENERGY_SCHEMA,
    )

    result = (
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )
    )

    assert result.count() == 3

    distinct_grains = (
        result
        .select(
            "geography_key",
            "gold_timestamp",
        )
        .distinct()
        .count()
    )

    assert distinct_grains == 3


def test_duplicate_weather_grain_is_rejected(
    spark: SparkSession,
) -> None:
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    duplicated_row = weather_row(
        geography_key="COUNTRY_ES",
        timestamp=timestamp,
        geography_level="COUNTRY",
        geography_name="España",
    )

    weather = spark.createDataFrame(
        [
            duplicated_row,
            duplicated_row,
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp,
                geography_level="COUNTRY",
                geography_name="España",
                wind=100.0,
            )
        ],
        ENERGY_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="duplicated Gold grains",
    ):
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )


def test_duplicate_energy_grain_is_rejected(
    spark: SparkSession,
) -> None:
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp,
                geography_level="COUNTRY",
                geography_name="España",
            )
        ],
        WEATHER_SCHEMA,
    )

    duplicated_row = energy_row(
        geography_key="COUNTRY_ES",
        timestamp=timestamp,
        geography_level="COUNTRY",
        geography_name="España",
        wind=100.0,
    )

    energy = spark.createDataFrame(
        [
            duplicated_row,
            duplicated_row,
        ],
        ENERGY_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="duplicated Gold grains",
    ):
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )


def test_contradictory_geography_is_rejected(
    spark: SparkSession,
) -> None:
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="SAME_KEY",
                timestamp=timestamp,
                geography_level="COUNTRY",
                geography_name="España",
            )
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="SAME_KEY",
                timestamp=timestamp,
                geography_level="PENINSULA",
                geography_name="Península",
                demand=500.0,
            )
        ],
        ENERGY_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="contradictory canonical geography",
    ):
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )


def test_unsupported_geography_level_is_rejected(
    spark: SparkSession,
) -> None:
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = spark.createDataFrame(
        [
            weather_row(
                geography_key="INVALID",
                timestamp=timestamp,
                geography_level="PROVINCE",
                geography_name="Madrid",
            )
        ],
        WEATHER_SCHEMA,
    )

    energy = spark.createDataFrame(
        [
            energy_row(
                geography_key="COUNTRY_ES",
                timestamp=timestamp,
                geography_level="COUNTRY",
                geography_name="España",
                wind=100.0,
            )
        ],
        ENERGY_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="unsupported geography_level",
    ):
        integrate_country_15min_weather_energy(
            weather,
            energy,
        )