import os

import pytest
from pyspark.sql import SparkSession

from silver.common import (
    decimal_comma_to_double,
    deduplicate,
    get_bronze_dataset_path,
    get_required_env,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-common-tests")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_get_required_env_returns_value(monkeypatch):
    monkeypatch.setenv("TEST_REQUIRED_ENV", "value")

    assert get_required_env("TEST_REQUIRED_ENV") == "value"


def test_get_required_env_fails_when_missing(monkeypatch):
    monkeypatch.delenv(
        "TEST_REQUIRED_ENV",
        raising=False,
    )

    with pytest.raises(RuntimeError):
        get_required_env("TEST_REQUIRED_ENV")


def test_get_bronze_dataset_path(monkeypatch):
    monkeypatch.setenv(
        "MINIO_BUCKET",
        "energy-lakehouse",
    )

    result = get_bronze_dataset_path(
        source="aemet",
        dataset="stations",
    )

    assert result == (
        "s3a://energy-lakehouse/"
        "bronze/aemet/stations/"
    )


def test_decimal_comma_to_double(spark):
    df = spark.createDataFrame(
        [
            ("12,5",),
            ("3.25",),
            ("",),
            (None,),
        ],
        ["raw_value"],
    )

    result = (
        df.select(
            decimal_comma_to_double(
                "raw_value"
            ).alias("value")
        )
        .collect()
    )

    assert result[0]["value"] == 12.5
    assert result[1]["value"] == 3.25
    assert result[2]["value"] is None
    assert result[3]["value"] is None




def test_deduplicate_by_natural_key(spark):
    df = spark.createDataFrame(
        [
            ("A", 1),
            ("A", 2),
            ("B", 3),
        ],
        ["station_id", "value"],
    )

    result = deduplicate(
        df,
        ["station_id"],
    )

    assert result.count() == 2


def test_deduplicate_fails_with_missing_key(spark):
    df = spark.createDataFrame(
        [
            ("A",),
        ],
        ["station_id"],
    )

    with pytest.raises(ValueError):
        deduplicate(
            df,
            ["missing_key"],
        )
