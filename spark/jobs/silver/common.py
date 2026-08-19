from __future__ import annotations

import logging
import os
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


LOGGER = logging.getLogger(__name__)


def get_required_env(name: str) -> str:
    """
    Return a required environment variable.

    Raises:
        RuntimeError: if the variable is not defined or is empty.
    """
    value = os.getenv(name)

    if value is None or not value.strip():
        raise RuntimeError(
            f"Required environment variable '{name}' is not configured."
        )

    return value.strip()


def get_spark_session(app_name: str) -> SparkSession:
    """
    Return the SparkSession configured through spark-defaults.conf.

    Iceberg, JDBC catalog, MinIO S3FileIO and S3A configuration are
    provided externally by the Spark runtime configuration.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def get_bronze_base_path() -> str:
    """
    Return the Bronze root path in MinIO.

    Example:
        s3a://<MINIO_BUCKET>/bronze
    """
    bucket = get_required_env("MINIO_BUCKET")

    return f"s3a://{bucket}/bronze"


def get_bronze_dataset_path(
    source: str,
    dataset: str,
) -> str:
    """
    Build the path for one Bronze dataset without hardcoding the bucket.
    """
    if not source.strip():
        raise ValueError("source cannot be empty.")

    if not dataset.strip():
        raise ValueError("dataset cannot be empty.")

    return (
        f"{get_bronze_base_path()}/"
        f"{source.strip()}/"
        f"{dataset.strip()}/"
    )


def read_bronze_json(
    spark: SparkSession,
    source: str,
    dataset: str,
    *,
    multiline: bool = True,
) -> DataFrame:
    """
    Read a JSON Bronze dataset from MinIO.

    Bronze JSON files validated during the Silver preparation can use
    formatted/multiline JSON, therefore multiline=True is the default.
    """
    path = get_bronze_dataset_path(
        source=source,
        dataset=dataset,
    )

    LOGGER.info(
        "Reading Bronze JSON dataset from %s",
        path,
    )

    return (
        spark.read
        .option(
            "multiLine",
            str(multiline).lower(),
        )
        .json(path)
    )


def read_bronze_csv(
    spark: SparkSession,
    source: str,
    dataset: str,
    *,
    delimiter: str = ";",
    encoding: str = "windows-1252",
) -> DataFrame:
    """
    Read a CSV Bronze dataset from MinIO.

    CNIG Bronze CSV files were validated with semicolon delimiters and
    Windows-1252 compatible encoding.
    """
    path = get_bronze_dataset_path(
        source=source,
        dataset=dataset,
    )

    LOGGER.info(
        "Reading Bronze CSV dataset from %s",
        path,
    )

    return (
        spark.read
        .option("header", "true")
        .option("delimiter", delimiter)
        .option("encoding", encoding)
        .csv(path)
    )


def decimal_comma_to_double(
    column_name: str,
):
    """
    Convert a textual decimal-comma value to DOUBLE.

    Empty strings are converted to NULL.
    Non-numeric source values naturally result in NULL after casting;
    source-specific handling must be implemented by the corresponding
    Silver transformer when required.
    """
    value = F.trim(F.col(column_name))

    return (
        F.when(
            value == "",
            F.lit(None),
        )
        .otherwise(
            F.regexp_replace(
                value,
                ",",
                ".",
            ).cast("double")
        )
    )


def empty_string_to_null(
    column_name: str,
):
    """
    Convert empty/blank strings to NULL without imputing values.
    """
    value = F.trim(F.col(column_name))

    return F.when(
        value == "",
        F.lit(None),
    ).otherwise(value)


def deduplicate(
    df: DataFrame,
    natural_keys: Sequence[str],
) -> DataFrame:
    """
    Remove exact duplicates according to the approved natural key.

    No value is imputed and no aggregation is performed.
    """
    keys = list(natural_keys)

    if not keys:
        raise ValueError(
            "At least one natural key column is required."
        )

    missing = [
        key
        for key in keys
        if key not in df.columns
    ]

    if missing:
        raise ValueError(
            "Natural key columns not present in DataFrame: "
            f"{missing}"
        )

    return df.dropDuplicates(keys)


def count_null_keys(
    df: DataFrame,
    natural_keys: Sequence[str],
) -> int:
    """
    Count rows containing at least one NULL natural-key component.
    """
    keys = list(natural_keys)

    if not keys:
        raise ValueError(
            "At least one natural key column is required."
        )

    condition = F.lit(False)

    for key in keys:
        if key not in df.columns:
            raise ValueError(
                f"Natural key column '{key}' is not present."
            )

        condition = condition | F.col(key).isNull()

    return df.filter(condition).count()


def create_namespace_if_not_exists(
    spark: SparkSession,
    namespace: str,
) -> None:
    """
    Create an Iceberg namespace in the configured catalog.

    The namespace must be provided explicitly by the caller.
    """
    if not namespace.strip():
        raise ValueError(
            "namespace cannot be empty."
        )

    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {namespace}"
    )