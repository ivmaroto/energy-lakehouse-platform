from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.common import (
    decimal_comma_to_double,
    deduplicate,
    read_bronze_csv,
)


SOURCE = "cnig"

PROVINCES_DATASET = "provinces"
MUNICIPALITIES_DATASET = "municipalities"


def _require_columns(
    df: DataFrame,
    required_columns: list[str],
) -> None:
    """
    Validate that all required Bronze columns exist.
    """
    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required CNIG Bronze columns: {missing}"
        )


def _with_traceability(
    df: DataFrame,
) -> DataFrame:
    """
    Add Silver technical traceability fields.

    CNIG Bronze files do not contain a JSON metadata wrapper, so the
    ingestion timestamp is derived from the validated Bronze filename
    convention.
    """
    filename = F.input_file_name()

    ingestion_timestamp_text = F.regexp_extract(
        filename,
        r"_(\d{8}T\d{12}Z)_",
        1,
    )

    return (
        df
        .withColumn(
            "source",
            F.lit(SOURCE),
        )
        .withColumn(
            "ingestion_timestamp",
            F.to_timestamp(
                ingestion_timestamp_text,
                "yyyyMMdd'T'HHmmssSSSSSS'Z'",
            ),
        )
    )


def read_provinces_bronze(
    spark: SparkSession,
) -> DataFrame:
    """
    Read the CNIG provinces Bronze dataset.
    """
    return read_bronze_csv(
        spark=spark,
        source=SOURCE,
        dataset=PROVINCES_DATASET,
    )


def read_municipalities_bronze(
    spark: SparkSession,
) -> DataFrame:
    """
    Read the CNIG municipalities Bronze dataset.
    """
    return read_bronze_csv(
        spark=spark,
        source=SOURCE,
        dataset=MUNICIPALITIES_DATASET,
    )


def transform_provinces(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transform CNIG provinces Bronze data into the approved
    silver_cnig_provinces schema.
    """
    required_columns = [
        "COD_PROV",
        "PROVINCIA",
        "COD_CA",
        "COMUNIDAD_AUTONOMA",
        "CAPITAL",
    ]

    _require_columns(
        bronze_df,
        required_columns,
    )

    traced = _with_traceability(
        bronze_df,
    )

    silver = traced.select(
        F.col("COD_PROV").alias(
            "province_code"
        ),
        F.col("PROVINCIA").alias(
            "province_name"
        ),
        F.col("COD_CA").alias(
            "autonomous_community_code"
        ),
        F.col("COMUNIDAD_AUTONOMA").alias(
            "autonomous_community_name"
        ),
        F.col("CAPITAL").alias(
            "capital_name"
        ),
        F.col("source"),
        F.col("ingestion_timestamp"),
    )

    return deduplicate(
        silver,
        ["province_code"],
    )


def transform_autonomous_communities(
    provinces_silver_df: DataFrame,
) -> DataFrame:
    """
    Derive the approved autonomous-community master from
    silver_cnig_provinces.
    """
    required_columns = [
        "autonomous_community_code",
        "autonomous_community_name",
        "source",
        "ingestion_timestamp",
    ]

    _require_columns(
        provinces_silver_df,
        required_columns,
    )

    silver = provinces_silver_df.select(
        "autonomous_community_code",
        "autonomous_community_name",
        "source",
        "ingestion_timestamp",
    )

    return deduplicate(
        silver,
        ["autonomous_community_code"],
    )


def transform_municipalities(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Transform CNIG municipalities Bronze data into the approved
    silver_cnig_municipalities schema.

    Natural key:
        municipality_ine_code <- COD_INE

    municipality_code <- COD_GEO is preserved but is not used as
    the natural key because validated Bronze evidence contains
    duplicate COD_GEO = 00000 values.
    """
    required_columns = [
        "COD_INE",
        "ID_REL",
        "COD_GEO",
        "COD_PROV",
        "PROVINCIA",
        "NOMBRE_ACTUAL",
        "POBLACION_MUNI",
        "SUPERFICIE",
        "PERIMETRO",
        "COD_INE_CAPITAL",
        "CAPITAL",
        "POBLACION_CAPITAL",
        "HOJA_MTN25",
        "LONGITUD_ETRS89_REGCAN95",
        "LATITUD_ETRS89_REGCAN95",
        "ORIGENCOOR",
        "ALTITUD",
        "ORIGENALTITUD",
    ]

    _require_columns(
        bronze_df,
        required_columns,
    )

    traced = _with_traceability(
        bronze_df,
    )

    silver = traced.select(
        F.col("COD_INE").alias(
            "municipality_ine_code"
        ),
        F.col("ID_REL").alias(
            "relation_id"
        ),
        F.col("COD_GEO").alias(
            "municipality_code"
        ),
        F.col("COD_PROV").alias(
            "province_code"
        ),
        F.col("PROVINCIA").alias(
            "province_name"
        ),
        F.col("NOMBRE_ACTUAL").alias(
            "municipality_name"
        ),
        F.col("POBLACION_MUNI")
        .cast("long")
        .alias(
            "municipality_population"
        ),
        decimal_comma_to_double(
            "SUPERFICIE"
        ).alias(
            "surface_area"
        ),
        F.col("PERIMETRO")
        .cast("long")
        .alias(
            "perimeter"
        ),
        F.col("COD_INE_CAPITAL").alias(
            "capital_ine_code"
        ),
        F.col("CAPITAL").alias(
            "capital_name"
        ),
        F.col("POBLACION_CAPITAL")
        .cast("long")
        .alias(
            "capital_population"
        ),
        F.col("HOJA_MTN25").alias(
            "mtn25_sheet"
        ),
        decimal_comma_to_double(
            "LONGITUD_ETRS89_REGCAN95"
        ).alias(
            "longitude"
        ),
        decimal_comma_to_double(
            "LATITUD_ETRS89_REGCAN95"
        ).alias(
            "latitude"
        ),
        F.col("ORIGENCOOR").alias(
            "coordinate_origin"
        ),
        decimal_comma_to_double(
            "ALTITUD"
        ).alias(
            "altitude"
        ),
        F.col("ORIGENALTITUD").alias(
            "altitude_origin"
        ),
        F.col("source"),
        F.col("ingestion_timestamp"),
    )

    return deduplicate(
        silver,
        ["municipality_ine_code"],
    )


def build_cnig_silver(
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Build the three approved CNIG Silver DataFrames.

    This function only performs Bronze -> Silver transformation.
    Iceberg table creation/writing is handled separately.
    """
    provinces_bronze = read_provinces_bronze(
        spark
    )

    municipalities_bronze = (
        read_municipalities_bronze(
            spark
        )
    )

    provinces_silver = transform_provinces(
        provinces_bronze
    )

    autonomous_communities_silver = (
        transform_autonomous_communities(
            provinces_silver
        )
    )

    municipalities_silver = (
        transform_municipalities(
            municipalities_bronze
        )
    )

    return (
        provinces_silver,
        autonomous_communities_silver,
        municipalities_silver,
    )