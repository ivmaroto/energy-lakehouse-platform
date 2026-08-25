from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gold.metrics import (
    country_15min_energy_metric_names,
)
from gold.weather import (
    COUNTRY_15MIN_WEATHER_METRICS,
)


# ============================================================================
# Approved Gold grain
# ============================================================================

COUNTRY_15MIN_GRAIN: tuple[str, ...] = (
    "geography_key",
    "gold_timestamp",
)

COUNTRY_15MIN_GEOGRAPHY_COLUMNS: tuple[str, ...] = (
    "geography_key",
    "geography_level",
    "geography_name",
)

VALID_COUNTRY_15MIN_GEOGRAPHY_LEVELS: tuple[str, ...] = (
    "COUNTRY",
    "PENINSULA",
)


# ============================================================================
# Generic validation helpers
# ============================================================================

def validate_required_columns(
    df: DataFrame,
    required_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """
    Validate that an intermediate Gold dataset contains every column required
    by the approved country × 15-minute integration contract.
    """
    missing = sorted(
        set(
            required_columns
        )
        - set(
            df.columns
        )
    )

    if missing:
        raise ValueError(
            f"{dataset_name} missing required columns: "
            f"{missing}"
        )


def validate_unique_grain(
    df: DataFrame,
    grain_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """
    Every prepared source must already contain exactly one logical row per
    target Gold grain before the weather-energy integration is performed.
    """
    grain = list(
        grain_columns
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


def validate_non_null_columns(
    df: DataFrame,
    columns: Iterable[str],
    dataset_name: str,
) -> None:
    """
    Structural Gold columns cannot be NULL.

    Metric NULL values remain valid and are never converted to zero.
    """
    column_list = list(
        columns
    )

    if not column_list:
        return

    condition = None

    for column in column_list:
        current = F.col(
            column
        ).isNull()

        if condition is None:
            condition = current
        else:
            condition = (
                condition
                |
                current
            )

    null_count = (
        df
        .filter(
            condition
        )
        .count()
    )

    if null_count != 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{null_count} rows with NULL structural fields."
        )


# ============================================================================
# Geography validation
# ============================================================================

def validate_allowed_geography_levels(
    df: DataFrame,
    dataset_name: str,
) -> None:
    """
    gold_fact_country_15min may contain only the two approved geographical
    levels:

        COUNTRY
        PENINSULA

    Spain and Peninsula remain distinct.
    """
    invalid_count = (
        df
        .filter(
            ~F.col(
                "geography_level"
            ).isin(
                list(
                    VALID_COUNTRY_15MIN_GEOGRAPHY_LEVELS
                )
            )
        )
        .count()
    )

    if invalid_count != 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{invalid_count} rows with unsupported "
            "geography_level values."
        )


def validate_geography_key_consistency(
    df: DataFrame,
    dataset_name: str,
) -> None:
    """
    A geography_key must always represent one deterministic geographical
    member.

    The same key cannot represent different geography levels or names across
    timestamps.
    """
    inconsistent_count = (
        df
        .select(
            "geography_key",
            "geography_level",
            "geography_name",
        )
        .distinct()
        .groupBy(
            "geography_key"
        )
        .count()
        .filter(
            F.col(
                "count"
            ) > 1
        )
        .count()
    )

    if inconsistent_count != 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{inconsistent_count} geography_key values "
            "mapped to multiple canonical geographies."
        )


def validate_cross_source_geography_consistency(
    weather_df: DataFrame,
    energy_df: DataFrame,
) -> None:
    """
    When weather and energy contain the same natural Gold key, their
    geographical attributes must describe the same canonical geography.

    A matching geography_key cannot represent COUNTRY in one source and
    PENINSULA in the other.
    """
    weather_geography = (
        weather_df
        .select(
            "geography_key",
            "gold_timestamp",
            "geography_level",
            "geography_name",
        )
        .alias(
            "weather"
        )
    )

    energy_geography = (
        energy_df
        .select(
            "geography_key",
            "gold_timestamp",
            "geography_level",
            "geography_name",
        )
        .alias(
            "energy"
        )
    )

    contradictory_count = (
        weather_geography
        .join(
            energy_geography,
            (
                F.col(
                    "weather.geography_key"
                )
                ==
                F.col(
                    "energy.geography_key"
                )
            )
            &
            (
                F.col(
                    "weather.gold_timestamp"
                )
                ==
                F.col(
                    "energy.gold_timestamp"
                )
            ),
            "inner",
        )
        .filter(
            (
                F.col(
                    "weather.geography_level"
                )
                !=
                F.col(
                    "energy.geography_level"
                )
            )
            |
            (
                F.col(
                    "weather.geography_name"
                )
                !=
                F.col(
                    "energy.geography_name"
                )
            )
        )
        .count()
    )

    if contradictory_count != 0:
        raise ValueError(
            "Country 15-minute integration contains "
            f"{contradictory_count} matching Gold grains "
            "with contradictory canonical geography."
        )


# ============================================================================
# Country/Peninsula × 15 min weather-energy integration
# ============================================================================

def integrate_country_15min_weather_energy(
    weather_df: DataFrame,
    energy_df: DataFrame,
) -> DataFrame:
    """
    Integrate already prepared 15-minute meteorological and ESIOS energy
    datasets.

    Expected weather grain:

        geography_key × gold_timestamp

    Expected energy grain:

        geography_key × gold_timestamp

    Approved geography:

        COUNTRY
        PENINSULA

    Integration policy:

        FULL OUTER JOIN

    This preserves valid coverage from either prepared source.

    Missing observations remain NULL.

    No:
        COALESCE(metric, 0)
        interpolation
        fabricated timestamp
        Spain -> Peninsula conversion
        Peninsula -> Spain conversion

    Both sources must already have reached the target Gold grain before this
    function is called.
    """
    weather_metrics = tuple(
        COUNTRY_15MIN_WEATHER_METRICS
    )

    energy_metrics = (
        country_15min_energy_metric_names()
    )

    # ------------------------------------------------------------------------
    # Input contract
    # ------------------------------------------------------------------------

    validate_required_columns(
        weather_df,
        [
            *COUNTRY_15MIN_GEOGRAPHY_COLUMNS,
            "gold_timestamp",
            *weather_metrics,
        ],
        "Gold country 15-minute weather",
    )

    validate_required_columns(
        energy_df,
        [
            *COUNTRY_15MIN_GEOGRAPHY_COLUMNS,
            "gold_timestamp",
            *energy_metrics,
        ],
        "Gold country 15-minute energy",
    )

    # ------------------------------------------------------------------------
    # Input grain
    # ------------------------------------------------------------------------

    validate_non_null_columns(
        weather_df,
        [
            *COUNTRY_15MIN_GEOGRAPHY_COLUMNS,
            "gold_timestamp",
        ],
        "Gold country 15-minute weather",
    )

    validate_non_null_columns(
        energy_df,
        [
            *COUNTRY_15MIN_GEOGRAPHY_COLUMNS,
            "gold_timestamp",
        ],
        "Gold country 15-minute energy",
    )

    validate_unique_grain(
        weather_df,
        COUNTRY_15MIN_GRAIN,
        "Gold country 15-minute weather",
    )

    validate_unique_grain(
        energy_df,
        COUNTRY_15MIN_GRAIN,
        "Gold country 15-minute energy",
    )

    # ------------------------------------------------------------------------
    # Geography integrity
    # ------------------------------------------------------------------------

    validate_allowed_geography_levels(
        weather_df,
        "Gold country 15-minute weather",
    )

    validate_allowed_geography_levels(
        energy_df,
        "Gold country 15-minute energy",
    )

    validate_geography_key_consistency(
        weather_df,
        "Gold country 15-minute weather",
    )

    validate_geography_key_consistency(
        energy_df,
        "Gold country 15-minute energy",
    )

    validate_cross_source_geography_consistency(
        weather_df,
        energy_df,
    )

    # ------------------------------------------------------------------------
    # Select only the approved integration contract
    # ------------------------------------------------------------------------

    weather = (
        weather_df
        .select(
            "geography_key",
            "gold_timestamp",
            "geography_level",
            "geography_name",
            *weather_metrics,
        )
        .alias(
            "weather"
        )
    )

    energy = (
        energy_df
        .select(
            "geography_key",
            "gold_timestamp",
            "geography_level",
            "geography_name",
            *energy_metrics,
        )
        .alias(
            "energy"
        )
    )

    # ------------------------------------------------------------------------
    # FULL OUTER integration
    #
    # Weather-only and energy-only valid observations are preserved.
    # Missing metrics remain NULL.
    # ------------------------------------------------------------------------

    joined = (
        weather
        .join(
            energy,
            (
                F.col(
                    "weather.geography_key"
                )
                ==
                F.col(
                    "energy.geography_key"
                )
            )
            &
            (
                F.col(
                    "weather.gold_timestamp"
                )
                ==
                F.col(
                    "energy.gold_timestamp"
                )
            ),
            "full_outer",
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
                    "weather.geography_key"
                ),
                F.col(
                    "energy.geography_key"
                ),
            ).alias(
                "geography_key"
            ),
            F.coalesce(
                F.col(
                    "weather.geography_level"
                ),
                F.col(
                    "energy.geography_level"
                ),
            ).alias(
                "geography_level"
            ),
            F.coalesce(
                F.col(
                    "weather.geography_name"
                ),
                F.col(
                    "energy.geography_name"
                ),
            ).alias(
                "geography_name"
            ),
            *[
                F.col(
                    f"weather.{metric}"
                ).alias(
                    metric
                )
                for metric
                in weather_metrics
            ],
            *[
                F.col(
                    f"energy.{metric}"
                ).alias(
                    metric
                )
                for metric
                in energy_metrics
            ],
        )
    )

    # ------------------------------------------------------------------------
    # Final Gold grain validation
    # ------------------------------------------------------------------------

    validate_non_null_columns(
        result,
        [
            *COUNTRY_15MIN_GEOGRAPHY_COLUMNS,
            "gold_timestamp",
        ],
        "Integrated Gold country 15-minute fact",
    )

    validate_unique_grain(
        result,
        COUNTRY_15MIN_GRAIN,
        "Integrated Gold country 15-minute fact",
    )

    validate_allowed_geography_levels(
        result,
        "Integrated Gold country 15-minute fact",
    )

    validate_geography_key_consistency(
        result,
        "Integrated Gold country 15-minute fact",
    )

    return result