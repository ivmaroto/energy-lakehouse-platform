import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from silver.open_meteo import (
    transform_weather_15min,
    transform_weather_hourly,
)


# ============================================================================
# Schemas
# ============================================================================

METADATA_HOURLY_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("province", StringType(), True),
        StructField("source", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ]
)


METADATA_15MIN_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("province", StringType(), True),
        StructField("source", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ]
)


HOURLY_TIMESERIES_SCHEMA = StructType(
    [
        StructField(
            "cloud_cover",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "dew_point_2m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "diffuse_radiation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "direct_normal_irradiance",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "direct_radiation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "precipitation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "pressure_msl",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "relative_humidity_2m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "shortwave_radiation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "sunshine_duration",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "surface_pressure",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "temperature_2m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "time",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "wind_direction_10m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "wind_gusts_10m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "wind_speed_10m",
            ArrayType(DoubleType(), True),
            True,
        ),
    ]
)


HISTORICAL_TIMESERIES_SCHEMA = StructType(
    [
        StructField(
            "time",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "wind_direction_120m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "wind_direction_80m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "wind_speed_120m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "wind_speed_80m",
            ArrayType(DoubleType(), True),
            True,
        ),
    ]
)


MINUTELY_15_TIMESERIES_SCHEMA = StructType(
    [
        StructField(
            "cloud_cover",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "dew_point_2m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "diffuse_radiation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "direct_normal_irradiance",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "direct_radiation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "precipitation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "pressure_msl",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "relative_humidity_2m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "shortwave_radiation",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "sunshine_duration",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "surface_pressure",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "temperature_2m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "time",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "wind_direction_10m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "wind_direction_120m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "wind_direction_80m",
            ArrayType(IntegerType(), True),
            True,
        ),
        StructField(
            "wind_gusts_10m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "wind_speed_10m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "wind_speed_120m",
            ArrayType(DoubleType(), True),
            True,
        ),
        StructField(
            "wind_speed_80m",
            ArrayType(DoubleType(), True),
            True,
        ),
    ]
)


HOURLY_DATA_SCHEMA = StructType(
    [
        StructField("elevation", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("hourly", HOURLY_TIMESERIES_SCHEMA, True),
    ]
)


HISTORICAL_DATA_SCHEMA = StructType(
    [
        StructField("elevation", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField(
            "hourly",
            HISTORICAL_TIMESERIES_SCHEMA,
            True,
        ),
    ]
)


MINUTELY_15_DATA_SCHEMA = StructType(
    [
        StructField("elevation", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField(
            "minutely_15",
            MINUTELY_15_TIMESERIES_SCHEMA,
            True,
        ),
    ]
)


WEATHER_HOURLY_SCHEMA = StructType(
    [
        StructField("data", HOURLY_DATA_SCHEMA, True),
        StructField("metadata", METADATA_HOURLY_SCHEMA, True),
    ]
)


WEATHER_HISTORICAL_SCHEMA = StructType(
    [
        StructField("data", HISTORICAL_DATA_SCHEMA, True),
        StructField("metadata", METADATA_HOURLY_SCHEMA, True),
    ]
)


WEATHER_15MIN_SCHEMA = StructType(
    [
        StructField("data", MINUTELY_15_DATA_SCHEMA, True),
        StructField("metadata", METADATA_15MIN_SCHEMA, True),
    ]
)


# ============================================================================
# Synthetic rows representing validated Bronze structures
# ============================================================================

HOURLY_DATA = {
    "elevation": 667.0,
    "latitude": 40.411,
    "longitude": -3.678,
    "hourly": {
        "cloud_cover": [10, 20],
        "dew_point_2m": [10.0, 11.0],
        "diffuse_radiation": [50.0, 60.0],
        "direct_normal_irradiance": [400.0, 500.0],
        "direct_radiation": [300.0, 350.0],
        "precipitation": [0.0, 0.1],
        "pressure_msl": [1012.0, 1011.5],
        "relative_humidity_2m": [55, 60],
        "shortwave_radiation": [450.0, 500.0],
        "sunshine_duration": [3600.0, 3500.0],
        "surface_pressure": [940.0, 939.5],
        "temperature_2m": [25.0, 26.0],
        "time": [
            "2026-08-18T14:00",
            "2026-08-18T15:00",
        ],
        "wind_direction_10m": [180, 190],
        "wind_gusts_10m": [14.0, 15.0],
        "wind_speed_10m": [3.8, 4.2],
    },
}


HISTORICAL_DATA = {
    "elevation": 667.0,
    "latitude": 40.411,
    "longitude": -3.678,
    "hourly": {
        "time": [
            "2026-08-18T14:00",
            "2026-08-18T15:00",
        ],
        "wind_direction_120m": [190, 200],
        "wind_direction_80m": [185, 195],
        "wind_speed_120m": [8.5, 9.0],
        "wind_speed_80m": [7.0, 7.5],
    },
}


MINUTELY_15_DATA = {
    "elevation": 667.0,
    "latitude": 40.411,
    "longitude": -3.678,
    "minutely_15": {
        "cloud_cover": [10, 20],
        "dew_point_2m": [10.0, 11.0],
        "diffuse_radiation": [50.0, 60.0],
        "direct_normal_irradiance": [400.0, 500.0],
        "direct_radiation": [300.0, 350.0],
        "precipitation": [0.0, 0.1],
        "pressure_msl": [1012.0, 1011.5],
        "relative_humidity_2m": [55, 60],
        "shortwave_radiation": [450.0, 500.0],
        "sunshine_duration": [900.0, 900.0],
        "surface_pressure": [940.0, 939.5],
        "temperature_2m": [25.0, 25.5],
        "time": [
            "2026-08-18T14:00",
            "2026-08-18T14:15",
        ],
        "wind_direction_10m": [180, 190],
        "wind_direction_120m": [200, 210],
        "wind_direction_80m": [190, 200],
        "wind_gusts_10m": [14.0, 15.0],
        "wind_speed_10m": [3.8, 4.2],
        "wind_speed_120m": [8.5, 9.0],
        "wind_speed_80m": [7.0, 7.5],
    },
}


HOURLY_METADATA = {
    "station_id": "3195",
    "station_name": "MADRID, RETIRO",
    "province": "MADRID",
    "source": "open_meteo",
    "ingestion_timestamp": "2026-08-18T14:05:00",
}


MINUTELY_15_METADATA = {
    "location_id": "3195",
    "station_name": "MADRID, RETIRO",
    "province": "MADRID",
    "source": "open_meteo",
    "ingestion_timestamp": "2026-08-18T14:05:00",
}


# ============================================================================
# Spark fixture
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-open-meteo-tests")
        .getOrCreate()
    )

    yield session
    session.stop()


# ============================================================================
# weather_hourly
# ============================================================================

def test_transform_weather_hourly(spark):
    df = spark.createDataFrame(
        [
            (
                HOURLY_DATA,
                HOURLY_METADATA,
            ),
        ],
        schema=WEATHER_HOURLY_SCHEMA,
    )

    result = transform_weather_hourly(df)

    rows = (
        result
        .orderBy("observation_timestamp")
        .collect()
    )

    assert len(rows) == 2

    assert rows[0]["station_id"] == "3195"
    assert rows[0]["station_name"] == "MADRID, RETIRO"
    assert rows[0]["province"] == "MADRID"

    assert rows[0]["latitude"] == pytest.approx(40.411)
    assert rows[0]["longitude"] == pytest.approx(-3.678)
    assert rows[0]["elevation"] == pytest.approx(667.0)

    assert rows[0]["temperature_2m"] == pytest.approx(25.0)
    assert rows[0]["relative_humidity_2m"] == 55
    assert rows[0]["precipitation"] == pytest.approx(0.0)
    assert rows[0]["wind_speed_10m"] == pytest.approx(3.8)

    assert rows[0]["source"] == "open_meteo"
    assert rows[0]["ingestion_timestamp"] is not None
    assert rows[0]["observation_timestamp"] is not None


def test_weather_hourly_deduplicates(spark):
    single_data = dict(HOURLY_DATA)

    single_data["hourly"] = {
        key: [value[0]]
        for key, value in HOURLY_DATA["hourly"].items()
    }

    df = spark.createDataFrame(
        [
            (
                single_data,
                HOURLY_METADATA,
            ),
            (
                single_data,
                HOURLY_METADATA,
            ),
        ],
        schema=WEATHER_HOURLY_SCHEMA,
    )

    result = transform_weather_hourly(df)

    assert result.count() == 1


# ============================================================================
# ============================================================================





# ============================================================================
# weather_15min
# ============================================================================

def test_transform_weather_15min(spark):
    df = spark.createDataFrame(
        [
            (
                MINUTELY_15_DATA,
                MINUTELY_15_METADATA,
            ),
        ],
        schema=WEATHER_15MIN_SCHEMA,
    )

    result = transform_weather_15min(df)

    rows = (
        result
        .orderBy("observation_timestamp")
        .collect()
    )

    assert len(rows) == 2

    assert rows[0]["station_id"] == "3195"

    assert rows[0]["temperature_2m"] == pytest.approx(25.0)

    assert rows[0]["wind_speed_80m"] == pytest.approx(7.0)
    assert rows[0]["wind_direction_80m"] == 190

    assert rows[0]["wind_speed_120m"] == pytest.approx(8.5)
    assert rows[0]["wind_direction_120m"] == 200

    assert rows[0]["source"] == "open_meteo"
    assert rows[0]["observation_timestamp"] is not None


def test_weather_15min_deduplicates(spark):
    single_data = dict(MINUTELY_15_DATA)

    single_data["minutely_15"] = {
        key: [value[0]]
        for key, value in MINUTELY_15_DATA["minutely_15"].items()
    }

    df = spark.createDataFrame(
        [
            (
                single_data,
                MINUTELY_15_METADATA,
            ),
            (
                single_data,
                MINUTELY_15_METADATA,
            ),
        ],
        schema=WEATHER_15MIN_SCHEMA,
    )

    result = transform_weather_15min(df)

    assert result.count() == 1


# ============================================================================
# Structural validation
# ============================================================================

def test_missing_required_open_meteo_field_fails(spark):
    incomplete_data_schema = StructType(
        [
            StructField(
                "latitude",
                DoubleType(),
                True,
            ),
            StructField(
                "longitude",
                DoubleType(),
                True,
            ),
        ]
    )

    incomplete_schema = StructType(
        [
            StructField(
                "data",
                incomplete_data_schema,
                True,
            ),
            StructField(
                "metadata",
                METADATA_HOURLY_SCHEMA,
                True,
            ),
        ]
    )

    df = spark.createDataFrame(
        [
            (
                {
                    "latitude": 40.411,
                    "longitude": -3.678,
                },
                HOURLY_METADATA,
            ),
        ],
        schema=incomplete_schema,
    )

    with pytest.raises(ValueError):
        transform_weather_hourly(df)