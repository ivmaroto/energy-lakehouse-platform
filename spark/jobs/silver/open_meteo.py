from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.common import (
    deduplicate,
    read_bronze_json,
)


SOURCE = "open_meteo"

WEATHER_HOURLY_DATASET = "weather_hourly"
WEATHER_15MIN_DATASET = "weather_15min"


# ============================================================================
# Physical Silver schemas approved in 01_silver_design.md
# ============================================================================

HOURLY_METEOROLOGICAL_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "sunshine_duration",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
]




MINUTELY_15_METEOROLOGICAL_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "sunshine_duration",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "wind_speed_80m",
    "wind_direction_80m",
    "wind_speed_120m",
    "wind_direction_120m",
]


# ============================================================================
# Validation helpers
# ============================================================================

def _require_columns(
    df: DataFrame,
    required_columns: list[str],
) -> None:
    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required Open-Meteo Bronze columns: {missing}"
        )


def _require_struct_fields(
    df: DataFrame,
    struct_name: str,
    required_fields: list[str],
) -> None:
    if struct_name not in df.columns:
        raise ValueError(
            f"Missing required Open-Meteo Bronze struct: {struct_name}"
        )

    struct_field = df.schema[struct_name]

    if not hasattr(struct_field.dataType, "fieldNames"):
        raise ValueError(
            f"Open-Meteo Bronze field '{struct_name}' is not a struct"
        )

    available_fields = set(
        struct_field.dataType.fieldNames()
    )

    missing = [
        field
        for field in required_fields
        if field not in available_fields
    ]

    if missing:
        raise ValueError(
            f"Missing required fields in Open-Meteo Bronze "
            f"struct '{struct_name}': {missing}"
        )


# ============================================================================
# Bronze readers
# ============================================================================

def read_weather_hourly_bronze(
    spark: SparkSession,
) -> DataFrame:
    return read_bronze_json(
        spark=spark,
        source=SOURCE,
        dataset=WEATHER_HOURLY_DATASET,
        multiline=True,
    )




def read_weather_15min_bronze(
    spark: SparkSession,
) -> DataFrame:
    return read_bronze_json(
        spark=spark,
        source=SOURCE,
        dataset=WEATHER_15MIN_DATASET,
        multiline=True,
    )


# ============================================================================
# Generic array-to-observation transformation
# ============================================================================

def _explode_observations(
    bronze_df: DataFrame,
    *,
    temporal_struct: str,
    meteorological_columns: list[str],
    station_id_metadata_field: str,
) -> DataFrame:
    """
    Convert one Open-Meteo Bronze wrapper per location into one Silver row
    per timestamp.

    Open-Meteo returns time-series variables as parallel arrays. arrays_zip()
    keeps every variable aligned with its corresponding timestamp before
    explode().
    """

    _require_columns(
        bronze_df,
        [
            "data",
            "metadata",
        ],
    )

    _require_struct_fields(
        bronze_df,
        "data",
        [
            "elevation",
            "latitude",
            "longitude",
            temporal_struct,
        ],
    )

    _require_struct_fields(
        bronze_df,
        "metadata",
        [
            station_id_metadata_field,
            "station_name",
            "province",
            "source",
            "ingestion_timestamp",
        ],
    )

    temporal_schema = (
        bronze_df
        .schema["data"]
        .dataType[temporal_struct]
        .dataType
    )

    available_temporal_fields = set(
        temporal_schema.fieldNames()
    )

    required_temporal_fields = [
        "time",
        *meteorological_columns,
    ]

    missing_temporal_fields = [
        field
        for field in required_temporal_fields
        if field not in available_temporal_fields
    ]

    if missing_temporal_fields:
        raise ValueError(
            f"Missing required Open-Meteo fields in "
            f"data.{temporal_struct}: {missing_temporal_fields}"
        )

    array_columns = [
        F.col(f"data.{temporal_struct}.time").alias(
            "time"
        )
    ]

    for column_name in meteorological_columns:
        array_columns.append(
            F.col(
                f"data.{temporal_struct}.{column_name}"
            ).alias(
                column_name
            )
        )

    zipped = bronze_df.select(
        F.col(
            f"metadata.{station_id_metadata_field}"
        ).alias(
            "station_id"
        ),
        F.col(
            "metadata.station_name"
        ).alias(
            "station_name"
        ),
        F.col(
            "metadata.province"
        ).alias(
            "province"
        ),
        F.col(
            "data.latitude"
        ).cast(
            "double"
        ).alias(
            "latitude"
        ),
        F.col(
            "data.longitude"
        ).cast(
            "double"
        ).alias(
            "longitude"
        ),
        F.col(
            "data.elevation"
        ).cast(
            "double"
        ).alias(
            "elevation"
        ),
        F.col(
            "metadata.source"
        ).alias(
            "source"
        ),
        F.to_timestamp(
            F.col(
                "metadata.ingestion_timestamp"
            )
        ).alias(
            "ingestion_timestamp"
        ),
        F.arrays_zip(
            *array_columns
        ).alias(
            "_observations"
        ),
    )

    exploded = (
        zipped
        .withColumn(
            "_observation",
            F.explode(
                F.col("_observations")
            ),
        )
        .drop(
            "_observations"
        )
    )

    expressions = [
        F.col("station_id"),
        F.to_timestamp(
            F.col("_observation.time")
        ).alias(
            "observation_timestamp"
        ),
        F.col("station_name"),
        F.col("province"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("elevation"),
    ]

    for column_name in meteorological_columns:
        expressions.append(
            F.col(
                f"_observation.{column_name}"
            ).alias(
                column_name
            )
        )

    expressions.extend(
        [
            F.col("source"),
            F.col("ingestion_timestamp"),
        ]
    )

    return exploded.select(
        *expressions
    )


# ============================================================================
# weather_hourly -> silver_open_meteo_hourly
# ============================================================================

def transform_weather_hourly(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Bronze:
        weather_hourly

    Silver:
        silver_open_meteo_hourly

    Granularity:
        1 hour

    Natural key:
        station_id + observation_timestamp
    """

    silver = _explode_observations(
        bronze_df,
        temporal_struct="hourly",
        meteorological_columns=HOURLY_METEOROLOGICAL_COLUMNS,
        station_id_metadata_field="station_id",
    )

    return deduplicate(
        silver,
        [
            "station_id",
            "observation_timestamp",
        ],
    )


# ============================================================================
# ============================================================================



# ============================================================================
# weather_15min -> silver_open_meteo_15min
# ============================================================================

def transform_weather_15min(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Bronze:
        weather_15min

    Silver:
        silver_open_meteo_15min

    Bronze metadata:
        location_id

    Silver normalization:
        location_id -> station_id

    Granularity:
        15 minutes

    Natural key:
        station_id + observation_timestamp
    """

    silver = _explode_observations(
        bronze_df,
        temporal_struct="minutely_15",
        meteorological_columns=MINUTELY_15_METEOROLOGICAL_COLUMNS,
        station_id_metadata_field="location_id",
    )

    return deduplicate(
        silver,
        [
            "station_id",
            "observation_timestamp",
        ],
    )


# ============================================================================
# Complete Open-Meteo Silver build
# ============================================================================

def build_open_meteo_silver(
    spark: SparkSession,
) -> tuple[
    DataFrame,
    DataFrame,
]:
    """
    Build the two active Open-Meteo Silver DataFrames.

    Bronze -> Silver transformation only.
    """

    hourly_bronze = read_weather_hourly_bronze(
        spark
    )

    weather_15min_bronze = read_weather_15min_bronze(
        spark
    )

    hourly_silver = transform_weather_hourly(
        hourly_bronze
    )

    weather_15min_silver = transform_weather_15min(
        weather_15min_bronze
    )

    return (
        hourly_silver,
        weather_15min_silver,
    )
