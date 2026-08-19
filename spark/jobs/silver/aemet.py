from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.common import (
    decimal_comma_to_double,
    deduplicate,
    read_bronze_json,
)


SOURCE = "aemet"

STATIONS_DATASET = "stations"
DAILY_DATASET = "daily_climatological_values"
CURRENT_OBSERVATIONS_DATASET = "current_observations"


# ---------------------------------------------------------------------------
# Validated Bronze schemas
# ---------------------------------------------------------------------------

STATION_COLUMNS = [
    "indicativo",
    "nombre",
    "provincia",
    "altitud",
    "latitud",
    "longitud",
    "indsinop",
]

DAILY_COLUMNS = [
    "altitud",
    "dir",
    "fecha",
    "horaHrMax",
    "horaHrMin",
    "horaPIntMax",
    "horaPresMax",
    "horaPresMin",
    "horaracha",
    "horatmax",
    "horatmin",
    "hrMax",
    "hrMedia",
    "hrMin",
    "indicativo",
    "nombre",
    "pintMax",
    "prec",
    "presMax",
    "presMin",
    "provincia",
    "racha",
    "sol",
    "tmax",
    "tmed",
    "tmin",
    "velmedia",
]

CURRENT_OBSERVATION_COLUMNS = [
    "alt",
    "dmax",
    "dmaxu",
    "dv",
    "dvu",
    "fint",
    "geo700",
    "geo850",
    "geo925",
    "hr",
    "idema",
    "inso",
    "lat",
    "lon",
    "nieve",
    "pacutp",
    "pliqt",
    "prec",
    "pres",
    "pres_nmar",
    "psoltp",
    "rviento",
    "stddv",
    "stddvu",
    "stdvv",
    "stdvvu",
    "ta",
    "tamax",
    "tamin",
    "tpr",
    "ts",
    "tss20cm",
    "tss5cm",
    "ubi",
    "vis",
    "vmax",
    "vmaxu",
    "vv",
    "vvu",
]


# Daily fields validated as numeric measurements represented as strings.
DAILY_NUMERIC_COLUMNS = [
    "altitud",
    "dir",
    "hrMax",
    "hrMedia",
    "hrMin",
    "pintMax",
    "prec",
    "presMax",
    "presMin",
    "racha",
    "sol",
    "tmax",
    "tmed",
    "tmin",
    "velmedia",
]


# ---------------------------------------------------------------------------
# Common validation / traceability
# ---------------------------------------------------------------------------

def _require_columns(
    df: DataFrame,
    required_columns: list[str],
) -> None:
    """
    Validate that all required validated Bronze columns are present.
    """
    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required AEMET Bronze columns: {missing}"
        )


def _with_traceability(
    df: DataFrame,
) -> DataFrame:
    """
    Add Silver technical traceability using Bronze metadata.
    """
    if "_bronze_ingestion_timestamp" in df.columns:
        ingestion_timestamp = F.to_timestamp(
            F.col("_bronze_ingestion_timestamp")
        )
    else:
        ingestion_timestamp = F.lit(None).cast("timestamp")

    return (
        df
        .withColumn(
            "source",
            F.lit(SOURCE),
        )
        .withColumn(
            "ingestion_timestamp",
            ingestion_timestamp,
        )
    )


def _read_all_json_objects(
    spark: SparkSession,
    dataset: str,
) -> DataFrame:
    """
    Read AEMET Bronze JSON wrappers and flatten their data arrays.

    Bronze structure:
        {
            "data": [...],
            "metadata": {...}
        }

    Silver transformations operate on one row per source record while
    preserving the Bronze ingestion timestamp for traceability.
    """
    wrapper_df = read_bronze_json(
        spark=spark,
        source=SOURCE,
        dataset=dataset,
        multiline=True,
    )

    required_wrapper_columns = [
        "data",
        "metadata",
    ]

    missing = [
        column
        for column in required_wrapper_columns
        if column not in wrapper_df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required AEMET Bronze wrapper columns: {missing}"
        )

    return (
        wrapper_df
        .select(
            F.explode_outer("data").alias("record"),
            F.col("metadata.ingestion_timestamp").alias(
                "_bronze_ingestion_timestamp"
            ),
        )
        .select(
            "record.*",
            "_bronze_ingestion_timestamp",
        )
    )


# ---------------------------------------------------------------------------
# Coordinate normalization
# ---------------------------------------------------------------------------

def _aemet_coordinate_to_decimal(
    column_name: str,
    *,
    degree_digits: int,
):
    """
    Convert an AEMET DMS coordinate to decimal degrees.

    Expected source structure:
        latitude  -> DDMMSSH
        longitude -> DDDMMSSH

    where H is one of:
        N, S, E, W

    Invalid structural values become NULL and can subsequently be
    classified by Silver data-quality controls.
    """
    value = F.upper(F.trim(F.col(column_name)))

    pattern = (
        rf"^(\d{{{degree_digits}}})"
        r"(\d{2})"
        r"(\d{2})"
        r"([NSEW])$"
    )

    degrees = F.regexp_extract(
        value,
        pattern,
        1,
    ).cast("double")

    minutes = F.regexp_extract(
        value,
        pattern,
        2,
    ).cast("double")

    seconds = F.regexp_extract(
        value,
        pattern,
        3,
    ).cast("double")

    hemisphere = F.regexp_extract(
        value,
        pattern,
        4,
    )

    valid = value.rlike(pattern)

    decimal_value = (
        degrees
        + minutes / F.lit(60.0)
        + seconds / F.lit(3600.0)
    )

    signed_value = (
        F.when(
            hemisphere.isin("S", "W"),
            -decimal_value,
        )
        .otherwise(decimal_value)
    )

    return (
        F.when(
            valid,
            signed_value,
        )
        .otherwise(F.lit(None).cast("double"))
    )


# ---------------------------------------------------------------------------
# Bronze readers
# ---------------------------------------------------------------------------

def read_stations_bronze(
    spark: SparkSession,
) -> DataFrame:
    return _read_all_json_objects(
        spark,
        STATIONS_DATASET,
    )


def read_daily_climatology_bronze(
    spark: SparkSession,
) -> DataFrame:
    return _read_all_json_objects(
        spark,
        DAILY_DATASET,
    )


def read_current_observations_bronze(
    spark: SparkSession,
) -> DataFrame:
    return _read_all_json_objects(
        spark,
        CURRENT_OBSERVATIONS_DATASET,
    )


# ---------------------------------------------------------------------------
# Stations
# ---------------------------------------------------------------------------

def transform_stations(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transform AEMET station reference data to Silver.

    Approved structural normalization:
        indicativo -> station_id
        latitud    -> latitude
        longitud   -> longitude

    Natural key:
        station_id
    """
    _require_columns(
        bronze_df,
        STATION_COLUMNS,
    )

    traced = _with_traceability(
        bronze_df,
    )

    silver = traced.select(
        F.col("indicativo").alias(
            "station_id"
        ),
        F.col("nombre"),
        F.col("provincia"),
        decimal_comma_to_double(
            "altitud"
        ).alias(
            "altitud"
        ),
        _aemet_coordinate_to_decimal(
            "latitud",
            degree_digits=2,
        ).alias(
            "latitude"
        ),
        _aemet_coordinate_to_decimal(
            "longitud",
            degree_digits=3,
        ).alias(
            "longitude"
        ),
        F.col("indsinop"),
        F.col("source"),
        F.col("ingestion_timestamp"),
    )

    return deduplicate(
        silver,
        ["station_id"],
    )


# ---------------------------------------------------------------------------
# Daily climatology
# ---------------------------------------------------------------------------

def transform_daily_climatology(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transform AEMET daily climatological data to Silver.

    Approved structural normalization:
        indicativo -> station_id
        fecha      -> observation_date

    Meteorological AEMET field names are otherwise preserved.

    Natural key:
        station_id + observation_date
    """
    _require_columns(
        bronze_df,
        DAILY_COLUMNS,
    )

    traced = _with_traceability(
        bronze_df,
    )

    expressions = [
        F.col("indicativo").alias(
            "station_id"
        ),
        F.to_date(
            F.col("fecha"),
            "yyyy-MM-dd",
        ).alias(
            "observation_date"
        ),
    ]

    for column_name in DAILY_COLUMNS:
        if column_name in {
            "indicativo",
            "fecha",
        }:
            continue

        if column_name in DAILY_NUMERIC_COLUMNS:
            expressions.append(
                decimal_comma_to_double(
                    column_name
                ).alias(
                    column_name
                )
            )
        else:
            expressions.append(
                F.col(column_name)
            )

    expressions.extend(
        [
            F.col("source"),
            F.col("ingestion_timestamp"),
        ]
    )

    silver = traced.select(
        *expressions
    )

    return deduplicate(
        silver,
        [
            "station_id",
            "observation_date",
        ],
    )


# ---------------------------------------------------------------------------
# Current observations
# ---------------------------------------------------------------------------

def transform_current_observations(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transform AEMET current observations to Silver.

    Approved structural normalization:
        idema -> station_id
        fint  -> observation_timestamp
        lat   -> latitude
        lon   -> longitude

    Remaining meteorological field names are preserved.

    Natural key:
        station_id + observation_timestamp
    """
    _require_columns(
        bronze_df,
        CURRENT_OBSERVATION_COLUMNS,
    )

    traced = _with_traceability(
        bronze_df,
    )

    expressions = [
        F.col("idema").alias(
            "station_id"
        ),
        F.to_timestamp(
            F.col("fint")
        ).alias(
            "observation_timestamp"
        ),
        F.col("lat")
        .cast("double")
        .alias(
            "latitude"
        ),
        F.col("lon")
        .cast("double")
        .alias(
            "longitude"
        ),
    ]

    for column_name in CURRENT_OBSERVATION_COLUMNS:
        if column_name in {
            "idema",
            "fint",
            "lat",
            "lon",
        }:
            continue

        expressions.append(
            F.col(column_name)
        )

    expressions.extend(
        [
            F.col("source"),
            F.col("ingestion_timestamp"),
        ]
    )

    silver = traced.select(
        *expressions
    )

    return deduplicate(
        silver,
        [
            "station_id",
            "observation_timestamp",
        ],
    )


# ---------------------------------------------------------------------------
# Complete AEMET Silver build
# ---------------------------------------------------------------------------

def build_aemet_silver(
    spark: SparkSession,
) -> tuple[
    DataFrame,
    DataFrame,
    DataFrame,
]:
    """
    Build the three approved AEMET Silver DataFrames.

    This function performs Bronze -> Silver transformation only.
    Iceberg persistence belongs to the subsequent implementation step.
    """
    stations_bronze = (
        read_stations_bronze(
            spark
        )
    )

    daily_bronze = (
        read_daily_climatology_bronze(
            spark
        )
    )

    current_bronze = (
        read_current_observations_bronze(
            spark
        )
    )

    stations_silver = (
        transform_stations(
            stations_bronze
        )
    )

    daily_silver = (
        transform_daily_climatology(
            daily_bronze
        )
    )

    current_silver = (
        transform_current_observations(
            current_bronze
        )
    )

    return (
        stations_silver,
        daily_silver,
        current_silver,
    )