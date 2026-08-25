from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.metrics import hourly_energy_metric_names
from gold.weather import PROVINCE_HOURLY_WEATHER_METRICS


# ============================================================================
# Approved Province × hour analytical grain
# ============================================================================

PROVINCE_HOURLY_GRAIN: tuple[str, ...] = (
    "province_code",
    "gold_timestamp",
)

PROVINCE_GEOGRAPHY_COLUMNS: tuple[str, ...] = (
    "province_code",
    "province_name",
    "autonomous_community_code",
    "autonomous_community_name",
)


# ============================================================================
# General validation helpers
# ============================================================================

def validate_required_columns(
    df: DataFrame,
    required_columns: tuple[str, ...] | list[str] | set[str],
    dataset_name: str,
) -> None:
    """
    Validate that a DataFrame contains every column required by the approved
    Gold integration.
    """
    required = set(
        required_columns
    )

    available = set(
        df.columns
    )

    missing = sorted(
        required - available
    )

    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{missing}"
        )


def validate_unique_grain(
    df: DataFrame,
    *,
    grain_columns: tuple[str, ...] | list[str],
    dataset_name: str,
) -> None:
    """
    Validate that a DataFrame contains at most one row per approved Gold grain.

    Every source must reach its analytical grain BEFORE integration.
    """
    grain = list(
        grain_columns
    )

    validate_required_columns(
        df,
        grain,
        dataset_name,
    )

    duplicate_count = (
        df
        .groupBy(
            *grain
        )
        .count()
        .filter(
            F.col(
                "count"
            ) > 1
        )
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicate_count} duplicated Gold grains."
        )


def validate_non_null_grain(
    df: DataFrame,
    *,
    grain_columns: tuple[str, ...] | list[str],
    dataset_name: str,
) -> None:
    """
    Natural Gold grain columns must never be NULL.
    """
    grain = list(
        grain_columns
    )

    validate_required_columns(
        df,
        grain,
        dataset_name,
    )

    null_condition = None

    for column_name in grain:
        current_condition = F.col(
            column_name
        ).isNull()

        if null_condition is None:
            null_condition = current_condition
        else:
            null_condition = (
                null_condition
                |
                current_condition
            )

    null_count = (
        df
        .filter(
            null_condition
        )
        .count()
    )

    if null_count != 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{null_count} rows with NULL Gold grain."
        )


# ============================================================================
# Province geography consistency
# ============================================================================

def validate_province_geography_consistency(
    weather_df: DataFrame,
    energy_df: DataFrame,
) -> None:
    """
    Validate that matching Province × hour records do not disagree about
    canonical geography.

    Both input products have already passed through the approved Silver/Gold
    geographical normalization. A contradictory canonical mapping must fail
    explicitly rather than being silently coalesced.
    """
    validate_required_columns(
        weather_df,
        PROVINCE_GEOGRAPHY_COLUMNS,
        "Gold Province × hour weather",
    )

    validate_required_columns(
        energy_df,
        PROVINCE_GEOGRAPHY_COLUMNS,
        "Gold Province × hour energy",
    )

    weather_geography = (
        weather_df
        .select(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        )
        .dropDuplicates()
        .alias(
            "weather"
        )
    )

    energy_geography = (
        energy_df
        .select(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        )
        .dropDuplicates()
        .alias(
            "energy"
        )
    )

    contradictions = (
        weather_geography
        .join(
            energy_geography,
            on="province_code",
            how="inner",
        )
        .filter(
            (
                F.col(
                    "weather.province_name"
                ).isNotNull()
                &
                F.col(
                    "energy.province_name"
                ).isNotNull()
                &
                (
                    F.col(
                        "weather.province_name"
                    )
                    !=
                    F.col(
                        "energy.province_name"
                    )
                )
            )
            |
            (
                F.col(
                    "weather.autonomous_community_code"
                ).isNotNull()
                &
                F.col(
                    "energy.autonomous_community_code"
                ).isNotNull()
                &
                (
                    F.col(
                        "weather.autonomous_community_code"
                    )
                    !=
                    F.col(
                        "energy.autonomous_community_code"
                    )
                )
            )
            |
            (
                F.col(
                    "weather.autonomous_community_name"
                ).isNotNull()
                &
                F.col(
                    "energy.autonomous_community_name"
                ).isNotNull()
                &
                (
                    F.col(
                        "weather.autonomous_community_name"
                    )
                    !=
                    F.col(
                        "energy.autonomous_community_name"
                    )
                )
            )
        )
        .count()
    )

    if contradictions != 0:
        raise ValueError(
            "Province × hour Gold integration found "
            f"{contradictions} contradictory canonical geography mappings."
        )


# ============================================================================
# Province × hour weather ↔ energy integration
# ============================================================================

def integrate_province_hourly_weather_energy(
    weather_df: DataFrame,
    energy_df: DataFrame,
) -> DataFrame:
    """
    Integrate the approved Province × hour meteorological and energy products.

    Approved integration policy:

        Weather Province × hour
                FULL OUTER JOIN
        Energy Province × hour

        ON:
            province_code
            gold_timestamp

    Rules:
        - both inputs must already contain at most one row per
          (province_code, gold_timestamp);
        - valid coverage from either source is preserved;
        - absent metrics remain NULL;
        - NULL is never converted to zero;
        - canonical geography is coalesced between both sources;
        - contradictory canonical geography fails explicitly;
        - the integration must not multiply records;
        - the final result must contain exactly one row per
          Province × hour natural key.
    """
    weather_metrics = tuple(
        PROVINCE_HOURLY_WEATHER_METRICS
    )

    energy_metrics = tuple(
        hourly_energy_metric_names()
    )

    weather_required = (
        PROVINCE_GEOGRAPHY_COLUMNS
        +
        (
            "gold_timestamp",
        )
        +
        weather_metrics
    )

    energy_required = (
        PROVINCE_GEOGRAPHY_COLUMNS
        +
        (
            "gold_timestamp",
        )
        +
        energy_metrics
    )

    validate_required_columns(
        weather_df,
        weather_required,
        "Gold Province × hour weather",
    )

    validate_required_columns(
        energy_df,
        energy_required,
        "Gold Province × hour energy",
    )

    validate_non_null_grain(
        weather_df,
        grain_columns=PROVINCE_HOURLY_GRAIN,
        dataset_name=(
            "Gold Province × hour weather"
        ),
    )

    validate_non_null_grain(
        energy_df,
        grain_columns=PROVINCE_HOURLY_GRAIN,
        dataset_name=(
            "Gold Province × hour energy"
        ),
    )

    validate_unique_grain(
        weather_df,
        grain_columns=PROVINCE_HOURLY_GRAIN,
        dataset_name=(
            "Gold Province × hour weather"
        ),
    )

    validate_unique_grain(
        energy_df,
        grain_columns=PROVINCE_HOURLY_GRAIN,
        dataset_name=(
            "Gold Province × hour energy"
        ),
    )

    validate_province_geography_consistency(
        weather_df,
        energy_df,
    )

    weather = (
        weather_df
        .alias(
            "weather"
        )
    )

    energy = (
        energy_df
        .alias(
            "energy"
        )
    )

    joined = (
        weather
        .join(
            energy,
            on=[
                weather[
                    "province_code"
                ]
                ==
                energy[
                    "province_code"
                ],

                weather[
                    "gold_timestamp"
                ]
                ==
                energy[
                    "gold_timestamp"
                ],
            ],
            how="full_outer",
        )
    )

    result = (
        joined
        .select(
            F.coalesce(
                F.col(
                    "weather.gold_timestamp"
                ),
                F.col(
                    "energy.gold_timestamp"
                ),
            ).alias(
                "gold_timestamp"
            ),

            F.coalesce(
                F.col(
                    "weather.province_code"
                ),
                F.col(
                    "energy.province_code"
                ),
            ).alias(
                "province_code"
            ),

            F.coalesce(
                F.col(
                    "weather.province_name"
                ),
                F.col(
                    "energy.province_name"
                ),
            ).alias(
                "province_name"
            ),

            F.coalesce(
                F.col(
                    "weather.autonomous_community_code"
                ),
                F.col(
                    "energy.autonomous_community_code"
                ),
            ).alias(
                "autonomous_community_code"
            ),

            F.coalesce(
                F.col(
                    "weather.autonomous_community_name"
                ),
                F.col(
                    "energy.autonomous_community_name"
                ),
            ).alias(
                "autonomous_community_name"
            ),

            *[
                F.col(
                    f"weather.{metric_name}"
                ).cast(
                    "double"
                ).alias(
                    metric_name
                )
                for metric_name
                in weather_metrics
            ],

            *[
                F.col(
                    f"energy.{metric_name}"
                ).cast(
                    "double"
                ).alias(
                    metric_name
                )
                for metric_name
                in energy_metrics
            ],

            F.col(
                "weather.temperature_source"
            ).alias(
                "temperature_source"
            ),

            F.col(
                "weather.humidity_source"
            ).alias(
                "humidity_source"
            ),

            F.col(
                "weather.precipitation_source"
            ).alias(
                "precipitation_source"
            ),
        )
    )

    validate_non_null_grain(
        result,
        grain_columns=PROVINCE_HOURLY_GRAIN,
        dataset_name=(
            "Integrated Gold Province × hour"
        ),
    )

    validate_unique_grain(
        result,
        grain_columns=PROVINCE_HOURLY_GRAIN,
        dataset_name=(
            "Integrated Gold Province × hour"
        ),
    )

    return result