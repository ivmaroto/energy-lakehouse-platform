from __future__ import annotations

import os
import sys

from datetime import datetime

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from gold.metrics import hourly_energy_metric_names
from gold.province_hourly_integration import (
    integrate_province_hourly_weather_energy,
)
from gold.weather import PROVINCE_HOURLY_WEATHER_METRICS


# ============================================================================
# Spark fixture
# ============================================================================
@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """
    Local Spark session for Gold Province × hour integration unit tests.

    Explicitly use the active Python interpreter for PySpark workers.
    This avoids Windows trying to execute the Linux-style "python3"
    command.
    """
    python_executable = sys.executable

    os.environ[
        "PYSPARK_PYTHON"
    ] = python_executable

    os.environ[
        "PYSPARK_DRIVER_PYTHON"
    ] = python_executable

    session = (
        SparkSession.builder
        .master(
            "local[2]"
        )
        .appName(
            "test-gold-province-hourly-integration"
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
# Test schemas
# ============================================================================

WEATHER_SCHEMA = StructType(
    [
        StructField(
            "gold_timestamp",
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
            True,
        ),
        StructField(
            "autonomous_community_code",
            StringType(),
            True,
        ),
        StructField(
            "autonomous_community_name",
            StringType(),
            True,
        ),
        *[
            StructField(
                metric_name,
                DoubleType(),
                True,
            )
            for metric_name
            in PROVINCE_HOURLY_WEATHER_METRICS
        ],
        StructField(
            "temperature_source",
            StringType(),
            True,
        ),
        StructField(
            "humidity_source",
            StringType(),
            True,
        ),
        StructField(
            "precipitation_source",
            StringType(),
            True,
        ),
    ]
)


ENERGY_SCHEMA = StructType(
    [
        StructField(
            "gold_timestamp",
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
            True,
        ),
        StructField(
            "autonomous_community_code",
            StringType(),
            True,
        ),
        StructField(
            "autonomous_community_name",
            StringType(),
            True,
        ),
        *[
            StructField(
                metric_name,
                DoubleType(),
                True,
            )
            for metric_name
            in hourly_energy_metric_names()
        ],
    ]
)


# ============================================================================
# Test data helpers
# ============================================================================

def weather_row(
    *,
    province_code: str,
    gold_timestamp: datetime,
    province_name: str = "Provincia Test",
    autonomous_community_code: str = "01",
    autonomous_community_name: str = "CCAA Test",
    temperature: float | None = 20.0,
) -> dict:
    """
    Build one valid Province × hour meteorological record.
    """
    row = {
        "gold_timestamp": gold_timestamp,
        "province_code": province_code,
        "province_name": province_name,
        "autonomous_community_code": (
            autonomous_community_code
        ),
        "autonomous_community_name": (
            autonomous_community_name
        ),
        "temperature_source": (
            "AEMET"
            if temperature is not None
            else None
        ),
        "humidity_source": "AEMET",
        "precipitation_source": "AEMET",
    }

    for metric_name in PROVINCE_HOURLY_WEATHER_METRICS:
        row[
            metric_name
        ] = None

    row[
        "temperature"
    ] = temperature

    row[
        "humidity"
    ] = 50.0

    row[
        "precipitation"
    ] = 1.0

    row[
        "wind_speed_80m"
    ] = 8.0

    row[
        "wind_direction_80m"
    ] = 180.0

    row[
        "wind_speed_120m"
    ] = 10.0

    row[
        "wind_direction_120m"
    ] = 190.0

    row[
        "solar_radiation"
    ] = 300.0

    row[
        "direct_normal_irradiance"
    ] = 200.0

    return row


def energy_row(
    *,
    province_code: str,
    gold_timestamp: datetime,
    province_name: str = "Provincia Test",
    autonomous_community_code: str = "01",
    autonomous_community_name: str = "CCAA Test",
    wind_generation_mwh: float | None = 100.0,
) -> dict:
    """
    Build one valid Province × hour energy record.
    """
    row = {
        "gold_timestamp": gold_timestamp,
        "province_code": province_code,
        "province_name": province_name,
        "autonomous_community_code": (
            autonomous_community_code
        ),
        "autonomous_community_name": (
            autonomous_community_name
        ),
    }

    for metric_name in hourly_energy_metric_names():
        row[
            metric_name
        ] = None

    row[
        "wind_generation_mwh"
    ] = wind_generation_mwh

    row[
        "solar_photovoltaic_generation_mwh"
    ] = 50.0

    row[
        "total_generation_mwh"
    ] = 500.0

    return row


def create_weather_df(
    spark: SparkSession,
    rows: list[dict],
):
    return spark.createDataFrame(
        rows,
        schema=WEATHER_SCHEMA,
    )


def create_energy_df(
    spark: SparkSession,
    rows: list[dict],
):
    return spark.createDataFrame(
        rows,
        schema=ENERGY_SCHEMA,
    )


# ============================================================================
# FULL OUTER integration
# ============================================================================

def test_integrates_matching_weather_and_energy_into_one_row(
    spark: SparkSession,
) -> None:
    """
    Matching Province × hour records must become exactly one integrated row.
    """
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = create_weather_df(
        spark,
        [
            weather_row(
                province_code="01",
                gold_timestamp=timestamp,
                temperature=22.5,
            ),
        ],
    )

    energy = create_energy_df(
        spark,
        [
            energy_row(
                province_code="01",
                gold_timestamp=timestamp,
                wind_generation_mwh=125.0,
            ),
        ],
    )

    result = (
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )
    )

    assert result.count() == 1

    row = result.first()

    assert row[
        "province_code"
    ] == "01"

    assert row[
        "gold_timestamp"
    ] == timestamp

    assert row[
        "temperature"
    ] == pytest.approx(
        22.5
    )

    assert row[
        "wind_generation_mwh"
    ] == pytest.approx(
        125.0
    )


def test_preserves_weather_without_energy(
    spark: SparkSession,
) -> None:
    """
    FULL OUTER must preserve valid meteorological coverage even when no
    energy observation exists for the same Province × hour.

    Missing energy metrics remain NULL.
    """
    timestamp = datetime(
        2026,
        8,
        23,
        11,
        0,
    )

    weather = create_weather_df(
        spark,
        [
            weather_row(
                province_code="02",
                gold_timestamp=timestamp,
                temperature=25.0,
            ),
        ],
    )

    energy = create_energy_df(
        spark,
        [],
    )

    result = (
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )
    )

    assert result.count() == 1

    row = result.first()

    assert row[
        "province_code"
    ] == "02"

    assert row[
        "temperature"
    ] == pytest.approx(
        25.0
    )

    assert row[
        "wind_generation_mwh"
    ] is None

    assert row[
        "total_generation_mwh"
    ] is None


def test_preserves_energy_without_weather(
    spark: SparkSession,
) -> None:
    """
    FULL OUTER must preserve valid energy coverage even when no
    meteorological observation exists for the same Province × hour.

    Missing meteorological metrics remain NULL.
    """
    timestamp = datetime(
        2026,
        8,
        23,
        12,
        0,
    )

    weather = create_weather_df(
        spark,
        [],
    )

    energy = create_energy_df(
        spark,
        [
            energy_row(
                province_code="03",
                gold_timestamp=timestamp,
                wind_generation_mwh=150.0,
            ),
        ],
    )

    result = (
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )
    )

    assert result.count() == 1

    row = result.first()

    assert row[
        "province_code"
    ] == "03"

    assert row[
        "wind_generation_mwh"
    ] == pytest.approx(
        150.0
    )

    assert row[
        "temperature"
    ] is None

    assert row[
        "temperature_source"
    ] is None


def test_full_outer_preserves_union_of_grains(
    spark: SparkSession,
) -> None:
    """
    The integrated product must contain the union of valid Province × hour
    grains from both sources.
    """
    hour_10 = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    hour_11 = datetime(
        2026,
        8,
        23,
        11,
        0,
    )

    hour_12 = datetime(
        2026,
        8,
        23,
        12,
        0,
    )

    weather = create_weather_df(
        spark,
        [
            weather_row(
                province_code="01",
                gold_timestamp=hour_10,
            ),
            weather_row(
                province_code="01",
                gold_timestamp=hour_11,
            ),
        ],
    )

    energy = create_energy_df(
        spark,
        [
            energy_row(
                province_code="01",
                gold_timestamp=hour_10,
            ),
            energy_row(
                province_code="01",
                gold_timestamp=hour_12,
            ),
        ],
    )

    result = (
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )
    )

    assert result.count() == 3

    timestamps = {
        row[
            "gold_timestamp"
        ]
        for row in result.select(
            "gold_timestamp"
        ).collect()
    }

    assert timestamps == {
        hour_10,
        hour_11,
        hour_12,
    }


# ============================================================================
# Prevention of record multiplication
# ============================================================================

def test_rejects_duplicated_weather_grain(
    spark: SparkSession,
) -> None:
    """
    Weather must already contain one row per Province × hour before joining.
    """
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = create_weather_df(
        spark,
        [
            weather_row(
                province_code="01",
                gold_timestamp=timestamp,
                temperature=20.0,
            ),
            weather_row(
                province_code="01",
                gold_timestamp=timestamp,
                temperature=21.0,
            ),
        ],
    )

    energy = create_energy_df(
        spark,
        [
            energy_row(
                province_code="01",
                gold_timestamp=timestamp,
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicated Gold grains",
    ):
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )


def test_rejects_duplicated_energy_grain(
    spark: SparkSession,
) -> None:
    """
    Energy must already contain one row per Province × hour before joining.
    """
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = create_weather_df(
        spark,
        [
            weather_row(
                province_code="01",
                gold_timestamp=timestamp,
            ),
        ],
    )

    energy = create_energy_df(
        spark,
        [
            energy_row(
                province_code="01",
                gold_timestamp=timestamp,
                wind_generation_mwh=100.0,
            ),
            energy_row(
                province_code="01",
                gold_timestamp=timestamp,
                wind_generation_mwh=200.0,
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicated Gold grains",
    ):
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )


# ============================================================================
# Geography consistency
# ============================================================================

def test_rejects_contradictory_canonical_geography(
    spark: SparkSession,
) -> None:
    """
    Matching province codes must never carry contradictory canonical
    province or autonomous-community geography.
    """
    timestamp = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    weather = create_weather_df(
        spark,
        [
            weather_row(
                province_code="01",
                gold_timestamp=timestamp,
                province_name="Provincia A",
                autonomous_community_code="10",
                autonomous_community_name="CCAA A",
            ),
        ],
    )

    energy = create_energy_df(
        spark,
        [
            energy_row(
                province_code="01",
                gold_timestamp=timestamp,
                province_name="Provincia B",
                autonomous_community_code="20",
                autonomous_community_name="CCAA B",
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="contradictory canonical geography",
    ):
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )


# ============================================================================
# Final grain validation
# ============================================================================

def test_final_result_contains_zero_duplicated_grains(
    spark: SparkSession,
) -> None:
    """
    A valid FULL OUTER integration must still contain exactly one row per
    (province_code, gold_timestamp).
    """
    hour_10 = datetime(
        2026,
        8,
        23,
        10,
        0,
    )

    hour_11 = datetime(
        2026,
        8,
        23,
        11,
        0,
    )

    weather = create_weather_df(
        spark,
        [
            weather_row(
                province_code="01",
                gold_timestamp=hour_10,
            ),
            weather_row(
                province_code="02",
                gold_timestamp=hour_10,
            ),
            weather_row(
                province_code="01",
                gold_timestamp=hour_11,
            ),
        ],
    )

    energy = create_energy_df(
        spark,
        [
            energy_row(
                province_code="01",
                gold_timestamp=hour_10,
            ),
            energy_row(
                province_code="02",
                gold_timestamp=hour_10,
            ),
            energy_row(
                province_code="01",
                gold_timestamp=hour_11,
            ),
        ],
    )

    result = (
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )
    )

    duplicated_grains = (
        result
        .groupBy(
            "province_code",
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

    assert duplicated_grains == 0
    assert result.count() == 3