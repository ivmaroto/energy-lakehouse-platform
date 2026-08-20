from __future__ import annotations

import json
import unicodedata
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


# ============================================================================
# Configuration
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

PROVINCE_ALIASES_PATH = (
    PROJECT_ROOT
    / "config"
    / "province_aliases.json"
)


# ============================================================================
# Province-name normalization
# ============================================================================

def normalize_geographical_name(
    value: str | None,
) -> str | None:
    """
    Normalize a geographical name for deterministic matching.

    Rules:
        - trim surrounding whitespace;
        - uppercase;
        - Unicode decomposition;
        - remove diacritics.

    This function is used for Python-side configuration processing,
    especially province_aliases.json.

    It does not translate geographical names and does not apply aliases.
    """
    if value is None:
        return None

    normalized = value.strip().upper()

    if not normalized:
        return None

    normalized = unicodedata.normalize(
        "NFKD",
        normalized,
    )

    normalized = "".join(
        character
        for character in normalized
        if not unicodedata.combining(character)
    )

    return normalized


def normalize_geographical_name_column(
    column: Column,
) -> Column:
    """
    Normalize a Spark geographical-name column using native Spark
    expressions only.

    Rules:
        - trim surrounding whitespace;
        - uppercase;
        - remove relevant Latin diacritics.

    Native Spark expressions are deliberately used instead of a Python UDF
    so normalization remains inside the Spark execution engine.
    """
    return F.translate(
        F.upper(
            F.trim(column)
        ),
        "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÑÇ",
        "AAAAAAEEEEIIIIOOOOOUUUUNC",
    )


# ============================================================================
# Controlled aliases
# ============================================================================

def load_province_aliases(
    path: Path = PROVINCE_ALIASES_PATH,
) -> dict[str, str]:
    """
    Load the controlled province aliases.

    Expected JSON structure:

        {
            "ALICANTE": "Alacant/Alicante",
            ...
        }

    Alias source names and canonical CNIG names are normalized before use so
    matching remains deterministic.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Province aliases file does not exist: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        aliases = json.load(file)

    if not isinstance(aliases, dict):
        raise ValueError(
            "province_aliases.json must contain a JSON object."
        )

    normalized_aliases: dict[str, str] = {}

    for source_name, canonical_name in aliases.items():
        if not isinstance(source_name, str):
            raise ValueError(
                "Province alias keys must be strings."
            )

        if not isinstance(canonical_name, str):
            raise ValueError(
                "Province alias values must be strings."
            )

        normalized_source = normalize_geographical_name(
            source_name
        )

        normalized_canonical = normalize_geographical_name(
            canonical_name
        )

        if (
            normalized_source is None
            or normalized_canonical is None
        ):
            raise ValueError(
                "Province aliases cannot contain empty names."
            )

        if normalized_source in normalized_aliases:
            raise ValueError(
                "Duplicate normalized province alias: "
                f"{normalized_source}"
            )

        normalized_aliases[
            normalized_source
        ] = normalized_canonical

    return normalized_aliases


# ============================================================================
# CNIG preparation
# ============================================================================

def prepare_cnig_provinces(
    cnig_provinces_df: DataFrame,
) -> DataFrame:
    """
    Prepare the CNIG province master for geographical matching.

    Required CNIG columns:
        - province_code
        - province_name
        - autonomous_community_code
        - autonomous_community_name
    """
    required_columns = [
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in cnig_provinces_df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required CNIG province columns: "
            f"{missing_columns}"
        )

    return (
        cnig_provinces_df
        .select(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        )
        .withColumn(
            "_cnig_normalized_province",
            normalize_geographical_name_column(
                F.col("province_name")
            ),
        )
        .dropDuplicates(
            ["province_code"]
        )
    )


# ============================================================================
# Source-province normalization
# ============================================================================

def normalize_source_province(
    df: DataFrame,
    *,
    source_province_column: str,
) -> DataFrame:
    """
    Add normalized geographical matching keys to a source DataFrame.

    Resolution order:
        1. deterministic normalization;
        2. controlled alias fallback when configured.

    The original source province column is never modified.
    """
    if source_province_column not in df.columns:
        raise ValueError(
            f"Missing source province column: "
            f"{source_province_column}"
        )

    aliases = load_province_aliases()

    alias_items: list[Column] = []

    for source_name, canonical_name in aliases.items():
        alias_items.extend(
            [
                F.lit(source_name),
                F.lit(canonical_name),
            ]
        )

    if alias_items:
        alias_map = F.create_map(
            *alias_items
        )
    else:
        alias_map = F.create_map(
            F.lit("__NO_ALIAS__"),
            F.lit("__NO_ALIAS__"),
        )

    normalized_source_column = (
        normalize_geographical_name_column(
            F.col(source_province_column)
        )
    )

    return (
        df
        .withColumn(
            "_source_normalized_province",
            normalized_source_column,
        )
        .withColumn(
            "_resolved_normalized_province",
            F.coalesce(
                alias_map[
                    F.col(
                        "_source_normalized_province"
                    )
                ],
                F.col(
                    "_source_normalized_province"
                ),
            ),
        )
    )


# ============================================================================
# Source -> CNIG geographical resolution
# ============================================================================

def enrich_with_cnig_province(
    df: DataFrame,
    cnig_provinces_df: DataFrame,
    *,
    source_province_column: str,
) -> DataFrame:
    """
    Resolve an existing source province against the canonical CNIG master.

    Resolution order:

        source province
            ->
        deterministic normalization
            ->
        controlled alias fallback when configured
            ->
        canonical CNIG province

    Output canonical columns:
        - province_code
        - province_name
        - autonomous_community_code
        - autonomous_community_name

    The original source province column is preserved unchanged for
    traceability.
    """
    normalized_source = normalize_source_province(
        df,
        source_province_column=source_province_column,
    )

    cnig = prepare_cnig_provinces(
        cnig_provinces_df
    )

    return (
        normalized_source
        .join(
            cnig,
            normalized_source[
                "_resolved_normalized_province"
            ]
            == cnig[
                "_cnig_normalized_province"
            ],
            how="left",
        )
        .drop(
            "_source_normalized_province",
            "_resolved_normalized_province",
            "_cnig_normalized_province",
        )
    )


# ============================================================================
# Validation
# ============================================================================

def validate_all_provinces_matched(
    df: DataFrame,
    *,
    dataset_name: str,
) -> None:
    """
    Require every geographical row to resolve to a canonical CNIG province.

    Validation fails when province_code or province_name remains NULL.
    """
    required_columns = [
        "province_code",
        "province_name",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing canonical province columns for "
            f"{dataset_name}: {missing_columns}"
        )

    unmatched_count = (
        df
        .filter(
            F.col("province_code").isNull()
            | F.col("province_name").isNull()
        )
        .count()
    )

    if unmatched_count != 0:
        raise ValueError(
            f"Geographical normalization failed for "
            f"{dataset_name}: "
            f"{unmatched_count} rows have no CNIG province match."
        )