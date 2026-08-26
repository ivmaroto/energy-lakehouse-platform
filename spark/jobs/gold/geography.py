from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.temporal import circular_mean_degrees

from silver.geography import (
    enrich_with_cnig_province,
    normalize_geographical_name_column,
    validate_all_provinces_matched,
)

# ============================================================================
# Canonical Gold geography keys
# ============================================================================

COUNTRY_ES_GEOGRAPHY_KEY = "COUNTRY:ES"
PENINSULA_ES_GEOGRAPHY_KEY = "PENINSULA:ES-PEN"

# ============================================================================
# Validation helpers
# ============================================================================

def validate_required_columns(
    df: DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> None:
    """
    Validate that all columns required by a Gold geographical
    transformation are present.
    """
    missing = sorted(
        required_columns.difference(df.columns)
    )

    if missing:
        raise ValueError(
            f"Missing required columns for {dataset_name}: "
            f"{missing}"
        )


# ============================================================================
# Province × hour
# ============================================================================

def aggregate_hourly_wind_points_to_province(
    df: DataFrame,
) -> DataFrame:
    """
    Aggregate previously prepared hourly Open-Meteo wind observations
    from point/station level to Province × hour.

    Approved rules:

    wind speed:
        points in province -> arithmetic AVG

    wind direction:
        points in province -> circular mean

    No temporal aggregation is performed here.
    """
    required_columns = {
        "station_id",
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
        "gold_timestamp",
        "wind_speed_80m",
        "wind_direction_80m",
        "wind_speed_120m",
        "wind_direction_120m",
    }

    validate_required_columns(
        df,
        required_columns,
        "hourly Open-Meteo wind",
    )

    return (
        df
        .groupBy(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "gold_timestamp",
        )
        .agg(
            F.avg(
                "wind_speed_80m"
            ).alias(
                "wind_speed_80m"
            ),
            circular_mean_degrees(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),
            F.avg(
                "wind_speed_120m"
            ).alias(
                "wind_speed_120m"
            ),
            circular_mean_degrees(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),
            F.countDistinct(
                "station_id"
            ).alias(
                "source_point_count"
            ),
        )
    )


def aggregate_hourly_scalars_points_to_province(
    df: DataFrame,
    *,
    metric_columns: list[str],
) -> DataFrame:
    """
    Aggregate scalar hourly Open-Meteo metrics from points to
    Province × hour using arithmetic averages.

    This helper does not apply meteorological fallback and does not
    perform joins with AEMET or ESIOS.
    """
    required_columns = {
        "station_id",
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
        "observation_timestamp",
        *metric_columns,
    }

    validate_required_columns(
        df,
        required_columns,
        "hourly Open-Meteo scalar metrics",
    )

    aggregations = [
        F.avg(
            column_name
        ).alias(
            column_name
        )
        for column_name in metric_columns
    ]

    aggregations.append(
        F.countDistinct(
            "station_id"
        ).alias(
            "source_point_count"
        )
    )

    return (
        df
        .withColumn(
            "gold_timestamp",
            F.date_trunc(
                "hour",
                F.col(
                    "observation_timestamp"
                ),
            ),
        )
        .groupBy(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "gold_timestamp",
        )
        .agg(
            *aggregations
        )
    )


# ============================================================================
# Spain × 15 min
# ============================================================================

def aggregate_15min_scalars_points_to_province(
    df: DataFrame,
    *,
    metric_columns: list[str],
) -> DataFrame:
    """
    First stage of the approved national weather aggregation:

        point -> province

    Scalar variables use arithmetic AVG.

    Open-Meteo observations already have 15-minute temporal grain,
    therefore no temporal aggregation is performed.
    """
    required_columns = {
        "station_id",
        "observation_timestamp",
        "province_code",
        "province_name",
        *metric_columns,
    }

    validate_required_columns(
        df,
        required_columns,
        "Open-Meteo 15-minute scalar metrics",
    )

    aggregations = [
        F.avg(
            column_name
        ).alias(
            column_name
        )
        for column_name in metric_columns
    ]

    aggregations.append(
        F.countDistinct(
            "station_id"
        ).alias(
            "source_point_count"
        )
    )

    return (
        df
        .groupBy(
            "province_code",
            "province_name",
            F.col(
                "observation_timestamp"
            ).alias(
                "gold_timestamp"
            ),
        )
        .agg(
            *aggregations
        )
    )


def aggregate_15min_directions_points_to_province(
    df: DataFrame,
) -> DataFrame:
    """
    First stage of the approved national wind-direction aggregation:

        point -> province

    Directions use circular mean.
    """
    required_columns = {
        "station_id",
        "observation_timestamp",
        "province_code",
        "province_name",
        "wind_direction_80m",
        "wind_direction_120m",
    }

    validate_required_columns(
        df,
        required_columns,
        "Open-Meteo 15-minute directions",
    )

    return (
        df
        .groupBy(
            "province_code",
            "province_name",
            F.col(
                "observation_timestamp"
            ).alias(
                "gold_timestamp"
            ),
        )
        .agg(
            circular_mean_degrees(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),
            circular_mean_degrees(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),
            F.countDistinct(
                "station_id"
            ).alias(
                "source_point_count"
            ),
        )
    )


def aggregate_15min_scalars_province_to_spain(
    df: DataFrame,
    *,
    metric_columns: list[str],
) -> DataFrame:
    """
    Second stage of the approved national weather aggregation:

        province -> Spain

    Every province contributes one provincial value to the national
    arithmetic mean.

    This deliberately avoids a direct average across all raw points.
    """
    required_columns = {
        "province_code",
        "gold_timestamp",
        *metric_columns,
    }

    validate_required_columns(
        df,
        required_columns,
        "provincial 15-minute scalar metrics",
    )

    aggregations = [
        F.avg(
            column_name
        ).alias(
            column_name
        )
        for column_name in metric_columns
    ]

    aggregations.append(
        F.countDistinct(
            "province_code"
        ).alias(
            "source_province_count"
        )
    )

    return (
        df
        .groupBy(
            "gold_timestamp"
        )
        .agg(
            *aggregations
        )
        .withColumn(
            "geography_level",
            F.lit(
                "COUNTRY"
            ),
        )
        .withColumn(
            "geography_name",
            F.lit(
                "España"
            ),
        )
    )


def aggregate_15min_directions_province_to_spain(
    df: DataFrame,
) -> DataFrame:
    """
    Second stage of the approved national wind-direction aggregation:

        province -> Spain

    Provincial directions are combined using circular mean.
    """
    required_columns = {
        "province_code",
        "gold_timestamp",
        "wind_direction_80m",
        "wind_direction_120m",
    }

    validate_required_columns(
        df,
        required_columns,
        "provincial 15-minute wind directions",
    )

    return (
        df
        .groupBy(
            "gold_timestamp"
        )
        .agg(
            circular_mean_degrees(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),
            circular_mean_degrees(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),
            F.countDistinct(
                "province_code"
            ).alias(
                "source_province_count"
            ),
        )
        .withColumn(
            "geography_level",
            F.lit(
                "COUNTRY"
            ),
        )
        .withColumn(
            "geography_name",
            F.lit(
                "España"
            ),
        )
    )


# ============================================================================
# Canonical CNIG province normalization
# ============================================================================

def normalize_gold_provinces(
    df: DataFrame,
    cnig_provinces_df: DataFrame,
    *,
    source_province_column: str,
) -> DataFrame:
    """
    Resolve a Gold source province against the canonical CNIG province
    master by reusing the geographical normalization already implemented
    and validated in Silver.

    Existing canonical geographical columns from the source are removed
    before resolution so CNIG remains the single canonical source for the
    resulting Gold geographical columns.

    Canonical output:
        - province_code
        - province_name
        - autonomous_community_code
        - autonomous_community_name
    """
    canonical_columns = [
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
    ]

    columns_to_drop = [
        column_name
        for column_name in canonical_columns
        if column_name in df.columns
    ]

    source = df.drop(
        *columns_to_drop
    )

    result = enrich_with_cnig_province(
        source,
        cnig_provinces_df,
        source_province_column=source_province_column,
    )

    validate_all_provinces_matched(
        result,
        dataset_name="Gold province normalization",
    )

    return result


# ============================================================================
# Canonical CNIG autonomous-community normalization
# ============================================================================

def prepare_cnig_autonomous_communities(
    cnig_autonomous_communities_df: DataFrame,
) -> DataFrame:
    """
    Prepare the canonical CNIG autonomous-community master.
    """
    required_columns = {
        "autonomous_community_code",
        "autonomous_community_name",
    }

    validate_required_columns(
        cnig_autonomous_communities_df,
        required_columns,
        "CNIG autonomous communities",
    )

    prepared = (
        cnig_autonomous_communities_df
        .select(
            "autonomous_community_code",
            "autonomous_community_name",
        )
        .withColumn(
            "_cnig_normalized_autonomous_community",
            normalize_geographical_name_column(
                F.col("autonomous_community_name")
            ),
        )
    )

    duplicate_count = (
        prepared
        .groupBy(
            "autonomous_community_code"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(
            "CNIG autonomous communities contains "
            f"{duplicate_count} duplicated "
            "autonomous_community_code values."
        )

    return prepared


def normalize_gold_autonomous_communities(
    df: DataFrame,
    cnig_autonomous_communities_df: DataFrame,
    *,
    source_autonomous_community_column: str,
) -> DataFrame:
    """
    Resolve or reuse canonical autonomous-community geography for Gold.

    Preferred path:
        - when Silver already provides canonical
          autonomous_community_code and autonomous_community_name,
          Gold reuses those columns directly.

    Fallback path:
        - when canonical columns are not present, resolve the source
          autonomous-community name against the canonical CNIG master
          using the same deterministic geographical-name normalization
          already implemented in Silver.

    No aliases are introduced in Gold.

    The original source column is preserved for traceability.
    """
    validate_required_columns(
        df,
        {
            source_autonomous_community_column,
        },
        "Gold autonomous-community source",
    )

    canonical_columns = {
        "autonomous_community_code",
        "autonomous_community_name",
    }

    available_canonical_columns = (
        canonical_columns.intersection(
            set(df.columns)
        )
    )

    # ------------------------------------------------------------------------
    # Preferred path
    #
    # Silver already contains canonical CNIG geography.
    # Gold must reuse it instead of normalizing it a second time.
    # ------------------------------------------------------------------------

    if available_canonical_columns == canonical_columns:
        unmatched_count = (
            df
            .filter(
                F.col(
                    "autonomous_community_code"
                ).isNull()
                |
                F.col(
                    "autonomous_community_name"
                ).isNull()
            )
            .count()
        )

        if unmatched_count != 0:
            raise ValueError(
                "Gold autonomous-community normalization failed: "
                f"{unmatched_count} rows contain NULL canonical "
                "autonomous-community geography from Silver."
            )

        return df

    # ------------------------------------------------------------------------
    # Inconsistent source schema
    #
    # Canonical geography must arrive either completely or not at all.
    # ------------------------------------------------------------------------

    if available_canonical_columns:
        missing_canonical_columns = sorted(
            canonical_columns
            - available_canonical_columns
        )

        raise ValueError(
            "Incomplete canonical autonomous-community geography "
            "received by Gold. Missing columns: "
            f"{missing_canonical_columns}"
        )

    # ------------------------------------------------------------------------
    # Fallback path
    #
    # Used only when the source does not already provide canonical CNIG
    # autonomous-community columns.
    # ------------------------------------------------------------------------

    source = (
        df
        .withColumn(
            "_source_normalized_autonomous_community",
            normalize_geographical_name_column(
                F.col(
                    source_autonomous_community_column
                )
            ),
        )
    )

    cnig = prepare_cnig_autonomous_communities(
        cnig_autonomous_communities_df
    )

    result = (
        source
        .join(
            F.broadcast(cnig),
            source[
                "_source_normalized_autonomous_community"
            ]
            == cnig[
                "_cnig_normalized_autonomous_community"
            ],
            how="left",
        )
        .drop(
            "_source_normalized_autonomous_community",
            "_cnig_normalized_autonomous_community",
        )
    )

    unmatched_count = (
        result
        .filter(
            F.col(
                "autonomous_community_code"
            ).isNull()
            |
            F.col(
                "autonomous_community_name"
            ).isNull()
        )
        .count()
    )

    if unmatched_count != 0:
        raise ValueError(
            "Gold autonomous-community normalization failed: "
            f"{unmatched_count} rows have no CNIG match."
        )

    return result


# ============================================================================
# Deterministic Gold geography keys
# ============================================================================

def add_deterministic_geography_key(
    df: DataFrame,
    *,
    geography_level: str,
    geography_code_column: str,
    target_column: str = "geography_key",
) -> DataFrame:
    """
    Add a deterministic SHA-256 Gold geography identifier.

    This helper is used for geographical levels whose Gold key
    serialization had not previously been fixed:

        PROVINCE
        AUTONOMOUS_COMMUNITY

    Spain and Peninsula keep their already validated canonical keys:

        COUNTRY:ES
        PENINSULA:ES-PEN

    The generated identifier is deterministic, stable and reproducible
    from:

        geography_level + geography_code
    """
    validate_required_columns(
        df,
        {
            geography_code_column,
        },
        "Gold deterministic geography key",
    )

    if geography_level not in {
        "PROVINCE",
        "AUTONOMOUS_COMMUNITY",
    }:
        raise ValueError(
            "Deterministic hashed geography keys are only approved "
            "for PROVINCE and AUTONOMOUS_COMMUNITY."
        )

    return df.withColumn(
        target_column,
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(
                    geography_level
                ),
                F.col(
                    geography_code_column
                ).cast(
                    "string"
                ),
            ),
            256,
        ),
    )