from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


# ============================================================================
# Lakehouse catalog and namespaces
# ============================================================================

CATALOG = "lakehouse"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"


# ============================================================================
# Project paths
# ============================================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[3]
)

GOLD_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "gold_config.json"
)


# ============================================================================
# Silver tables used by Gold
# ============================================================================

TABLE_SILVER_AEMET_CURRENT = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_aemet_current_observations"
)

TABLE_SILVER_OPEN_METEO_HOURLY = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_open_meteo_hourly"
)

TABLE_SILVER_OPEN_METEO_15MIN = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_open_meteo_15min"
)

TABLE_SILVER_ESIOS_ENERGY_HOURLY = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_esios_energy_hourly"
)

TABLE_SILVER_ESIOS_POWER_5MIN = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_esios_power_5min"
)

TABLE_SILVER_ESIOS_INSTALLED_CAPACITY_MONTHLY = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_esios_installed_capacity_monthly"
)

TABLE_SILVER_CNIG_PROVINCES = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_cnig_provinces"
)

TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES = (
    f"{CATALOG}.{SILVER_NAMESPACE}."
    "silver_cnig_autonomous_communities"
)


GOLD_SOURCE_TABLES = (
    TABLE_SILVER_AEMET_CURRENT,
    TABLE_SILVER_OPEN_METEO_HOURLY,
    TABLE_SILVER_OPEN_METEO_15MIN,
    TABLE_SILVER_ESIOS_ENERGY_HOURLY,
    TABLE_SILVER_ESIOS_POWER_5MIN,
    TABLE_SILVER_ESIOS_INSTALLED_CAPACITY_MONTHLY,
    TABLE_SILVER_CNIG_PROVINCES,
    TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES,
)


# ============================================================================
# Gold table names
# ============================================================================

TABLE_GOLD_FACT_PROVINCE_HOURLY = (
    f"{CATALOG}.{GOLD_NAMESPACE}."
    "gold_fact_province_hourly"
)

TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY = (
    f"{CATALOG}.{GOLD_NAMESPACE}."
    "gold_fact_installed_capacity_monthly"
)

TABLE_GOLD_FACT_COUNTRY_15MIN = (
    f"{CATALOG}.{GOLD_NAMESPACE}."
    "gold_fact_country_15min"
)

TABLE_GOLD_FACT_COUNTRY_5MIN = (
    f"{CATALOG}.{GOLD_NAMESPACE}."
    "gold_fact_country_5min"
)

TABLE_GOLD_DIM_TIME = (
    f"{CATALOG}.{GOLD_NAMESPACE}."
    "gold_dim_time"
)

TABLE_GOLD_DIM_GEOGRAPHY = (
    f"{CATALOG}.{GOLD_NAMESPACE}."
    "gold_dim_geography"
)


GOLD_TABLES = (
    TABLE_GOLD_FACT_PROVINCE_HOURLY,
    TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY,
    TABLE_GOLD_FACT_COUNTRY_15MIN,
    TABLE_GOLD_FACT_COUNTRY_5MIN,
    TABLE_GOLD_DIM_TIME,
    TABLE_GOLD_DIM_GEOGRAPHY,
)


# ============================================================================
# Spark
# ============================================================================

def get_spark_session(
    app_name: str = "gold-layer",
) -> SparkSession:
    """
    Return the SparkSession configured by the existing project runtime.

    Iceberg, JDBC catalog, MinIO and S3/S3A configuration are reused
    from the external Spark configuration already present in the project.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


# ============================================================================
# Generic Iceberg helpers
# ============================================================================

def table_exists(
    spark: SparkSession,
    table_name: str,
) -> bool:
    """
    Return whether an Iceberg table exists in the configured catalog.
    """
    if not table_name.strip():
        raise ValueError(
            "table_name cannot be empty."
        )

    return spark.catalog.tableExists(
        table_name
    )


def validate_table_exists(
    spark: SparkSession,
    table_name: str,
) -> None:
    """
    Fail explicitly when a required Iceberg table does not exist.
    """
    if not table_exists(
        spark=spark,
        table_name=table_name,
    ):
        raise RuntimeError(
            "Required Iceberg table does not exist: "
            f"{table_name}"
        )


# ============================================================================
# Silver access
# ============================================================================

def read_silver_table(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """
    Read a persisted Silver Iceberg table.

    Gold consumes Silver directly and does not rebuild Silver from Bronze.
    """
    validate_table_exists(
        spark=spark,
        table_name=table_name,
    )

    return spark.table(
        table_name
    )


def validate_gold_source_tables(
    spark: SparkSession,
) -> None:
    """
    Validate that every Silver table required by Gold exists.
    """
    missing_tables = [
        table_name
        for table_name in GOLD_SOURCE_TABLES
        if not table_exists(
            spark=spark,
            table_name=table_name,
        )
    ]

    if missing_tables:
        raise RuntimeError(
            "Required Silver tables for Gold are missing: "
            f"{missing_tables}"
        )


# ============================================================================
# Gold configuration
# ============================================================================

def load_gold_config() -> dict:
    """
    Load the Gold functional configuration from config/gold_config.json.
    """
    if not GOLD_CONFIG_PATH.exists():
        raise RuntimeError(
            "Gold configuration file does not exist: "
            f"{GOLD_CONFIG_PATH}"
        )

    with GOLD_CONFIG_PATH.open(
        "r",
        encoding="utf-8-sig",
    ) as file:
        config = json.load(file)

    if not isinstance(
        config,
        dict,
    ):
        raise ValueError(
            "Gold configuration must contain a JSON object."
        )

    return config


def get_esios_time_gap_hours() -> int:
    """
    Return the approved configurable ESIOS → Gold time gap.

    The value must come from config/gold_config.json and must not be
    hardcoded in transformation logic.
    """
    config = load_gold_config()

    key = "esios_time_gap_hours"

    if key not in config:
        raise RuntimeError(
            f"Missing required Gold configuration key: {key}"
        )

    value = config[key]

    if isinstance(
        value,
        bool,
    ) or not isinstance(
        value,
        int,
    ):
        raise ValueError(
            "esios_time_gap_hours must be an integer."
        )

    return value