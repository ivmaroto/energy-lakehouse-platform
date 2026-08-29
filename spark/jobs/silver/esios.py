from __future__ import annotations

from functools import reduce
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.common import (
    deduplicate,
    read_bronze_json,
)

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SOURCE = "esios"


# ============================================================================
# Validated Bronze datasets
# ============================================================================

ESIOS_INDICATORS_CONFIG = (
    Path(__file__).resolve().parents[3]
    / "config"
    / "esios_indicators.json"
)


def _load_active_esios_datasets() -> tuple[str, ...]:
    """
    Load the active ESIOS Bronze datasets from the shared
    project configuration.

    config/esios_indicators.json is the single source of truth
    for ingestion and Silver dataset scope.
    """

    with ESIOS_INDICATORS_CONFIG.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = json.load(file)

    datasets: list[str] = []

    for group in (
        "hourly",
        "monthly",
    ):
        indicators = config.get(group)

        if not isinstance(indicators, dict):
            raise ValueError(
                "Invalid or missing ESIOS indicator group: "
                f"{group}"
            )

        datasets.extend(
            str(dataset)
            for dataset in indicators.values()
        )

    if len(datasets) != len(set(datasets)):
        raise ValueError(
            "Duplicate ESIOS Bronze dataset configured."
        )

    return tuple(datasets)


ESIOS_DATASETS = _load_active_esios_datasets()


# ============================================================================
# Validated ESIOS classification
# ============================================================================

MAGNITUDE_ENERGY_ID = 13
MAGNITUDE_POWER_ID = 20

TIME_HOUR_ID = 4
TIME_MONTH_ID = 2


# ============================================================================
# Natural key
# ============================================================================

ESIOS_NATURAL_KEY = [
    "indicator_id",
    "esios_geo_id",
    "observation_timestamp",
]

ESIOS_SILVER_SCHEMA = StructType(
    [
        StructField("indicator_id", LongType(), True),
        StructField("dataset", StringType(), True),
        StructField("indicator_name", StringType(), True),
        StructField("indicator_short_name", StringType(), True),
        StructField("magnitude_id", LongType(), True),
        StructField("magnitude_name", StringType(), True),
        StructField("time_id", LongType(), True),
        StructField("time_name", StringType(), True),
        StructField("observation_timestamp", TimestampType(), True),
        StructField("source_datetime", TimestampType(), True),
        StructField("tz_time", TimestampType(), True),
        StructField("esios_geo_id", LongType(), True),
        StructField("esios_geo_name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("values_updated_at", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)


# ============================================================================
# Structural validation
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
            f"Missing required ESIOS Bronze columns: {missing}"
        )


def _require_struct_fields(
    df: DataFrame,
    struct_path: str,
    required_fields: list[str],
) -> None:
    """
    Validate required fields in a top-level struct.

    Currently used for:
        data
        metadata
    """
    if struct_path not in df.columns:
        raise ValueError(
            f"Missing required ESIOS Bronze struct: {struct_path}"
        )

    struct_field = df.schema[struct_path]

    if not hasattr(
        struct_field.dataType,
        "fieldNames",
    ):
        raise ValueError(
            f"ESIOS Bronze field '{struct_path}' is not a struct"
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
            f"Missing required fields in ESIOS Bronze struct "
            f"'{struct_path}': {missing}"
        )


# ============================================================================
# Bronze reader
# ============================================================================

def read_esios_dataset_bronze(
    spark: SparkSession,
    dataset: str,
) -> DataFrame:
    if dataset not in ESIOS_DATASETS:
        raise ValueError(
            f"Unknown validated ESIOS dataset: {dataset}"
        )

    return read_bronze_json(
        spark=spark,
        source=SOURCE,
        dataset=dataset,
        multiline=True,
    )


# ============================================================================
# Bronze -> normalized observation rows
# ============================================================================

def transform_esios_dataset(
    bronze_df: DataFrame,
    dataset: str,
) -> DataFrame:
    """
    Normalize one ESIOS Bronze dataset into observation-level rows.

    Empty values arrays generate zero Silver observations.
    No synthetic records are manufactured.
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
            "indicator",
        ],
    )

    indicator_schema = (
        bronze_df
        .schema["data"]
        .dataType["indicator"]
        .dataType
    )

    required_indicator_fields = {
        "id",
        "magnitud",
        "name",
        "short_name",
        "tiempo",
        "values",
        "values_updated_at",
    }

    available_indicator_fields = set(
        indicator_schema.fieldNames()
    )

    missing_indicator_fields = sorted(
        required_indicator_fields
        - available_indicator_fields
    )

    values_field = indicator_schema["values"]
    values_element_type = values_field.dataType.elementType

    if not isinstance(values_element_type, StructType):
        non_empty_values = (
            bronze_df
            .filter(
                F.size(
                    F.col("data.indicator.values")
                ) > 0
            )
            .count()
        )

        if non_empty_values != 0:
            raise ValueError(
                "ESIOS values contains records but Spark did not infer "
                "a STRUCT element type."
            )

        return bronze_df.sparkSession.createDataFrame(
            [],
            schema=ESIOS_SILVER_SCHEMA,
        )

    if missing_indicator_fields:
        raise ValueError(
            "Missing required ESIOS indicator fields: "
            f"{missing_indicator_fields}"
        )

    magnitude = F.element_at(
        F.col("data.indicator.magnitud"),
        1,
    )

    time_definition = F.element_at(
        F.col("data.indicator.tiempo"),
        1,
    )

    exploded = (
        bronze_df
        .select(
            F.col(
                "data.indicator.id"
            ).cast(
                "long"
            ).alias(
                "indicator_id"
            ),
            F.lit(
                dataset
            ).alias(
                "dataset"
            ),
            F.col(
                "data.indicator.name"
            ).alias(
                "indicator_name"
            ),
            F.col(
                "data.indicator.short_name"
            ).alias(
                "indicator_short_name"
            ),
            magnitude["id"]
            .cast("long")
            .alias(
                "magnitude_id"
            ),
            magnitude["name"]
            .alias(
                "magnitude_name"
            ),
            time_definition["id"]
            .cast("long")
            .alias(
                "time_id"
            ),
            time_definition["name"]
            .alias(
                "time_name"
            ),
            F.to_timestamp(
                F.col(
                    "data.indicator.values_updated_at"
                )
            ).alias(
                "values_updated_at"
            ),
            F.to_timestamp(
                F.col(
                    "metadata.ingestion_timestamp"
                )
            ).alias(
                "ingestion_timestamp"
            ),
            F.explode(
                F.col(
                    "data.indicator.values"
                )
            ).alias(
                "_value"
            ),
        )
    )

    silver = exploded.select(
        F.col(
            "indicator_id"
        ),
        F.col(
            "dataset"
        ),
        F.col(
            "indicator_name"
        ),
        F.col(
            "indicator_short_name"
        ),
        F.col(
            "magnitude_id"
        ),
        F.col(
            "magnitude_name"
        ),
        F.col(
            "time_id"
        ),
        F.col(
            "time_name"
        ),
        F.to_timestamp(
            F.col(
                "_value.datetime_utc"
            )
        ).alias(
            "observation_timestamp"
        ),
        F.to_timestamp(
            F.col(
                "_value.datetime"
            )
        ).alias(
            "source_datetime"
        ),
        F.to_timestamp(
            F.col(
                "_value.tz_time"
            )
        ).alias(
            "tz_time"
        ),
        F.col(
            "_value.geo_id"
        ).cast(
            "long"
        ).alias(
            "esios_geo_id"
        ),
        F.col(
            "_value.geo_name"
        ).alias(
            "esios_geo_name"
        ),
        F.col(
            "_value.value"
        ).cast(
            "double"
        ).alias(
            "value"
        ),
        F.col(
            "values_updated_at"
        ),
        F.lit(
            SOURCE
        ).alias(
            "source"
        ),
        F.col(
            "ingestion_timestamp"
        ),
    )

    return deduplicate(
        silver,
        ESIOS_NATURAL_KEY,
    )


# ============================================================================
# Build normalized observations for all active datasets
# ============================================================================

def build_all_esios_observations(
    spark: SparkSession,
) -> DataFrame:
    """
    Read and normalize every active ESIOS Bronze dataset.

    Dataset scope is defined by config/esios_indicators.json.
    """

    transformed = []

    for dataset in ESIOS_DATASETS:
        bronze_df = read_esios_dataset_bronze(
            spark,
            dataset,
        )

        silver_df = transform_esios_dataset(
            bronze_df,
            dataset,
        )

        transformed.append(
            silver_df
        )

    if not transformed:
        raise RuntimeError(
            "No validated ESIOS datasets configured."
        )

    combined = reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=False,
        ),
        transformed,
    )

    return deduplicate(
        combined,
        ESIOS_NATURAL_KEY,
    )


# ============================================================================
# ESIOS Silver families
# ============================================================================

def build_esios_energy_hourly(
    observations: DataFrame,
) -> DataFrame:
    """
    silver_esios_energy_hourly

    Validated classification:
        magnitude = EnergÃ­a  (13)
        time      = Hora     (4)
    """

    return observations.filter(
        (F.col("magnitude_id") == MAGNITUDE_ENERGY_ID)
        &
        (F.col("time_id") == TIME_HOUR_ID)
    )




def build_esios_installed_capacity_monthly(
    observations: DataFrame,
) -> DataFrame:
    """
    silver_esios_installed_capacity_monthly

    Validated classification:
        magnitude = Potencia  (20)
        time      = Mes       (2)
    """

    return observations.filter(
        (F.col("magnitude_id") == MAGNITUDE_POWER_ID)
        &
        (F.col("time_id") == TIME_MONTH_ID)
    )


# ============================================================================
# Complete ESIOS Silver build
# ============================================================================

def build_esios_silver(
    spark: SparkSession,
) -> tuple[
    DataFrame,
    DataFrame,
]:
    """
    Build the two active ESIOS Silver DataFrames.

    Bronze -> Silver transformation only.

    No temporal aggregation is performed.
    No geographical detail is manufactured.
    Empty source values arrays remain empty.
    """

    observations = build_all_esios_observations(
        spark
    )

    energy_hourly = build_esios_energy_hourly(
        observations
    )

    installed_capacity_monthly = (
        build_esios_installed_capacity_monthly(
            observations
        )
    )

    return (
        energy_hourly,
        installed_capacity_monthly,
    )
