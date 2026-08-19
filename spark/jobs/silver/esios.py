from __future__ import annotations

from functools import reduce

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

ESIOS_DATASETS = [
    "demanda_en_consumo",
    "demanda_medida_discriminacion_horaria_total",
    "demanda_real",
    "demanda_real_suma_generacion",
    "generacion_medida_carbon",
    "generacion_medida_ciclo_combinado",
    "generacion_medida_eolica_terrestre",
    "generacion_medida_gas_natural_cogeneracion",
    "generacion_medida_gas_natural_turbina_vapor",
    "generacion_medida_hidraulica",
    "generacion_medida_nuclear",
    "generacion_medida_otras_renovables",
    "generacion_medida_solar_fotovoltaica",
    "generacion_medida_solar_termica",
    "generacion_medida_total",
    "generacion_medida_total_tipo_produccion",
    "generacion_treal_carbon_nacional",
    "generacion_treal_ciclo_combinado_nacional",
    "generacion_treal_cogeneracion_residuos_nacional",
    "generacion_treal_consumo_bombeo_nacional",
    "generacion_treal_eolica_nacional",
    "generacion_treal_hidraulica_nacional",
    "generacion_treal_nuclear_nacional",
    "generacion_treal_solar_fotovoltaica_nacional",
    "generacion_treal_solar_termica_nacional",
    "generacion_treal_termica_renovable_nacional",
    "potencia_instalada_carbon",
    "potencia_instalada_ciclo_combinado",
    "potencia_instalada_eolica",
    "potencia_instalada_hidraulica",
    "potencia_instalada_nuclear",
    "potencia_instalada_otras_renovables",
    "potencia_instalada_solar_fotovoltaica",
    "potencia_instalada_solar_termica",
    "potencia_instalada_total_renovable",
]


# ============================================================================
# Validated ESIOS classification
# ============================================================================

MAGNITUDE_ENERGY_ID = 13
MAGNITUDE_POWER_ID = 20

TIME_HOUR_ID = 4
TIME_FIVE_MINUTES_ID = 219
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
# Build normalized observations for all 35 datasets
# ============================================================================

def build_all_esios_observations(
    spark: SparkSession,
) -> DataFrame:
    """
    Read and normalize every validated ESIOS Bronze dataset.

    The result still contains all three temporal families.
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
        magnitude = Energía  (13)
        time      = Hora     (4)
    """

    return observations.filter(
        (F.col("magnitude_id") == MAGNITUDE_ENERGY_ID)
        &
        (F.col("time_id") == TIME_HOUR_ID)
    )


def build_esios_power_5min(
    observations: DataFrame,
) -> DataFrame:
    """
    silver_esios_power_5min

    Validated classification:
        magnitude = Potencia       (20)
        time      = Cinco minutos  (219)
    """

    return observations.filter(
        (F.col("magnitude_id") == MAGNITUDE_POWER_ID)
        &
        (
            F.col("time_id")
            == TIME_FIVE_MINUTES_ID
        )
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
    DataFrame,
]:
    """
    Build the three approved ESIOS Silver DataFrames.

    Bronze -> Silver transformation only.

    No temporal aggregation is performed.
    No geographical detail is manufactured.
    Empty source values arrays remain empty.
    """

    observations = build_all_esios_observations(
        spark
    )

    energy_hourly = (
        build_esios_energy_hourly(
            observations
        )
    )

    power_5min = (
        build_esios_power_5min(
            observations
        )
    )

    installed_capacity_monthly = (
        build_esios_installed_capacity_monthly(
            observations
        )
    )

    return (
        energy_hourly,
        power_5min,
        installed_capacity_monthly,
    )