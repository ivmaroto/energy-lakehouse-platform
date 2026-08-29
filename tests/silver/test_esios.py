import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from silver.esios import (
    build_esios_energy_hourly,
    build_esios_installed_capacity_monthly,
    transform_esios_dataset,
)


MAGNITUDE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)


TIME_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
    ]
)


VALUE_SCHEMA = StructType(
    [
        StructField("datetime", StringType(), True),
        StructField("datetime_utc", StringType(), True),
        StructField("geo_id", LongType(), True),
        StructField("geo_name", StringType(), True),
        StructField("tz_time", StringType(), True),
        StructField("value", DoubleType(), True),
    ]
)


INDICATOR_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("magnitud", ArrayType(MAGNITUDE_SCHEMA), True),
        StructField("name", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("tiempo", ArrayType(TIME_SCHEMA), True),
        StructField("values", ArrayType(VALUE_SCHEMA), True),
        StructField("values_updated_at", StringType(), True),
    ]
)


DATA_SCHEMA = StructType(
    [
        StructField(
            "indicator",
            INDICATOR_SCHEMA,
            True,
        ),
    ]
)


METADATA_SCHEMA = StructType(
    [
        StructField(
            "ingestion_timestamp",
            StringType(),
            True,
        ),
    ]
)


ESIOS_SCHEMA = StructType(
    [
        StructField(
            "data",
            DATA_SCHEMA,
            True,
        ),
        StructField(
            "metadata",
            METADATA_SCHEMA,
            True,
        ),
    ]
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-esios-tests")
        .getOrCreate()
    )

    yield session
    session.stop()


def make_bronze_row(
    *,
    indicator_id,
    magnitude_id,
    magnitude_name,
    time_id,
    time_name,
    values,
):
    return (
        {
            "indicator": {
                "id": indicator_id,
                "magnitud": [
                    {
                        "id": magnitude_id,
                        "name": magnitude_name,
                    }
                ],
                "name": "TEST INDICATOR",
                "short_name": "TEST",
                "tiempo": [
                    {
                        "id": time_id,
                        "name": time_name,
                    }
                ],
                "values": values,
                "values_updated_at": "2026-08-18T14:05:00",
            }
        },
        {
            "ingestion_timestamp": "2026-08-18T14:10:00",
        },
    )


def test_transform_esios_dataset(spark):
    row = make_bronze_row(
        indicator_id=1001,
        magnitude_id=13,
        magnitude_name="Energía",
        time_id=4,
        time_name="Hora",
        values=[
            {
                "datetime": "2026-08-18T14:00:00+02:00",
                "datetime_utc": "2026-08-18T12:00:00Z",
                "geo_id": 8741,
                "geo_name": "Madrid",
                "tz_time": "2026-08-18T14:00:00+02:00",
                "value": 1234.5,
            }
        ],
    )

    df = spark.createDataFrame(
        [row],
        schema=ESIOS_SCHEMA,
    )

    result = transform_esios_dataset(
        df,
        "test_dataset",
    )

    record = result.first()

    assert result.count() == 1

    assert record["indicator_id"] == 1001
    assert record["dataset"] == "test_dataset"
    assert record["indicator_name"] == "TEST INDICATOR"
    assert record["indicator_short_name"] == "TEST"

    assert record["magnitude_id"] == 13
    assert record["magnitude_name"] == "Energía"

    assert record["time_id"] == 4
    assert record["time_name"] == "Hora"

    assert record["observation_timestamp"] is not None
    assert record["source_datetime"] is not None
    assert record["tz_time"] is not None

    assert record["esios_geo_id"] == 8741
    assert record["esios_geo_name"] == "Madrid"

    assert record["value"] == pytest.approx(1234.5)

    assert record["values_updated_at"] is not None
    assert record["source"] == "esios"
    assert record["ingestion_timestamp"] is not None


def test_empty_values_produces_zero_rows(spark):
    row = make_bronze_row(
        indicator_id=1002,
        magnitude_id=13,
        magnitude_name="Energía",
        time_id=4,
        time_name="Hora",
        values=[],
    )

    df = spark.createDataFrame(
        [row],
        schema=ESIOS_SCHEMA,
    )

    result = transform_esios_dataset(
        df,
        "empty_dataset",
    )

    assert result.count() == 0


def test_esios_natural_key_deduplicates(spark):
    value = {
        "datetime": "2026-08-18T14:00:00+02:00",
        "datetime_utc": "2026-08-18T12:00:00Z",
        "geo_id": 8741,
        "geo_name": "Madrid",
        "tz_time": "2026-08-18T14:00:00+02:00",
        "value": 1234.5,
    }

    row = make_bronze_row(
        indicator_id=1003,
        magnitude_id=13,
        magnitude_name="Energía",
        time_id=4,
        time_name="Hora",
        values=[
            value,
            value,
        ],
    )

    df = spark.createDataFrame(
        [row],
        schema=ESIOS_SCHEMA,
    )

    result = transform_esios_dataset(
        df,
        "duplicate_dataset",
    )

    assert result.count() == 1


def test_build_esios_energy_hourly(spark):
    df = spark.createDataFrame(
        [
            (
                1001,
                13,
                4,
            ),
            (
                1002,
                20,
                219,
            ),
            (
                1003,
                20,
                2,
            ),
        ],
        [
            "indicator_id",
            "magnitude_id",
            "time_id",
        ],
    )

    result = build_esios_energy_hourly(df)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["indicator_id"] == 1001




def test_build_esios_installed_capacity_monthly(spark):
    df = spark.createDataFrame(
        [
            (
                1001,
                13,
                4,
            ),
            (
                1002,
                20,
                219,
            ),
            (
                1003,
                20,
                2,
            ),
        ],
        [
            "indicator_id",
            "magnitude_id",
            "time_id",
        ],
    )

    result = build_esios_installed_capacity_monthly(df)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["indicator_id"] == 1003


def test_missing_required_esios_structure_fails(spark):
    incomplete_schema = StructType(
        [
            StructField(
                "data",
                StructType([]),
                True,
            ),
            StructField(
                "metadata",
                METADATA_SCHEMA,
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                {},
                {
                    "ingestion_timestamp":
                        "2026-08-18T14:10:00",
                },
            ),
        ],
        schema=incomplete_schema,
    )

    with pytest.raises(ValueError):
        transform_esios_dataset(
            df,
            "invalid_dataset",
        )