from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================================
# Approved Gold metric mappings
#
# Source of truth:
#     docs/Gold/01_gold_design.md
#
# Indicator IDs must not be added, removed, or substituted without an
# explicit Gold-design decision.
# ============================================================================


# ============================================================================
# Province Ã— hour
#
# Silver source:
#     silver_esios_energy_hourly
#
# Source magnitude:
#     Energy
#
# Gold rule:
#     metric_mwh = value
#
# No AVG.
# No SUM.
# No MW -> MWh conversion.
# ============================================================================

HOURLY_ENERGY_METRICS: dict[int, str] = {
    1159: "wind_generation_mwh",
    1161: "solar_photovoltaic_generation_mwh",
    1162: "solar_thermal_generation_mwh",
    10035: "hydraulic_generation_mwh",
    1153: "nuclear_generation_mwh",
    1156: "combined_cycle_generation_mwh",
    1158: "gas_natural_steam_turbine_generation_mwh",
    1164: "gas_natural_cogeneration_mwh",
    10036: "coal_generation_mwh",
    10041: "other_renewables_generation_mwh",
    10043: "total_generation_mwh",
}

HOURLY_ENERGY_EXCLUDED_INDICATORS: frozenset[int] = frozenset(
    {
        10195,
        1193,
        10267,
    }
)


# ============================================================================
# Autonomous Community Ã— month
#
# Silver source:
#     silver_esios_installed_capacity_monthly
#
# Source magnitude:
#     Power
#
# Gold rule:
#     installed_capacity_mw = value
#
# Capacity remains MW.
# No conversion to MWh.
# No temporal SUM(MW) across months.
# ============================================================================

INSTALLED_CAPACITY_METRICS: dict[int, str] = {
    1475: "hydraulic_installed_capacity_mw",
    1485: "wind_installed_capacity_mw",
    1486: "solar_photovoltaic_installed_capacity_mw",
    1487: "solar_thermal_installed_capacity_mw",
    10302: "renewable_total_installed_capacity_mw",
    1477: "nuclear_installed_capacity_mw",
    1478: "coal_installed_capacity_mw",
    1483: "combined_cycle_installed_capacity_mw",
    1488: "other_renewables_installed_capacity_mw",
}


# ============================================================================
# Approved meteorological metric names
#
# Selection only.
#
# Spatial/temporal aggregation and the AEMET -> Open-Meteo fallback are
# implemented by their corresponding Gold transformation components.
# ============================================================================

PROVINCE_HOURLY_WEATHER_METRICS: tuple[str, ...] = (
    "temperature",
    "humidity",
    "precipitation",
    "wind_speed_80m",
    "wind_direction_80m",
    "wind_speed_120m",
    "wind_direction_120m",
    "solar_radiation",
    "direct_normal_irradiance",
)



# ============================================================================
# General helpers
# ============================================================================

def validate_required_columns(
    df: DataFrame,
    required_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """
    Validate that every required input column is available.

    Gold must fail explicitly when an expected structural column is missing
    instead of silently producing an incomplete analytical product.
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


def selected_indicator_ids(
    metric_mapping: dict[int, str],
) -> tuple[int, ...]:
    """
    Return the approved indicator IDs in deterministic order.
    """
    return tuple(
        sorted(
            metric_mapping.keys()
        )
    )


def select_approved_indicators(
    df: DataFrame,
    metric_mapping: dict[int, str],
    *,
    indicator_column: str = "indicator_id",
    dataset_name: str = "Gold metric source",
) -> DataFrame:
    """
    Keep only indicators explicitly approved for a Gold product.

    No unapproved indicator is propagated implicitly.
    """
    validate_required_columns(
        df,
        {
            indicator_column,
        },
        dataset_name,
    )

    approved_ids = list(
        selected_indicator_ids(
            metric_mapping
        )
    )

    return df.filter(
        F.col(
            indicator_column
        ).isin(
            approved_ids
        )
    )


def validate_unique_indicator_observations(
    df: DataFrame,
    *,
    grain_columns: Iterable[str],
    indicator_column: str = "indicator_id",
    dataset_name: str,
) -> None:
    """
    Ensure one source observation per Gold grain + indicator before pivoting.

    Pivoting must not hide duplicate source rows by arbitrarily selecting one
    value. Any unexpected duplicate must be investigated before integration.
    """
    grain = list(
        grain_columns
    )

    validate_required_columns(
        df,
        [
            *grain,
            indicator_column,
        ],
        dataset_name,
    )

    duplicate_count = (
        df
        .groupBy(
            *grain,
            indicator_column,
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicate_count} duplicated "
            "grain + indicator combinations."
        )


def pivot_indicator_metrics(
    df: DataFrame,
    *,
    grain_columns: Iterable[str],
    metric_mapping: dict[int, str],
    value_column: str,
    indicator_column: str = "indicator_id",
    dataset_name: str,
) -> DataFrame:
    """
    Convert approved ESIOS indicators from long format to Gold wide format.

    Rules:
        - only approved indicator IDs participate;
        - source duplicates are rejected before pivot;
        - explicit zero remains zero;
        - missing observation remains NULL;
        - signs are preserved;
        - no COALESCE(metric, 0) is applied.

    The function does not perform temporal or spatial aggregation.
    """
    grain = list(
        grain_columns
    )

    validate_required_columns(
        df,
        [
            *grain,
            indicator_column,
            value_column,
        ],
        dataset_name,
    )

    selected = select_approved_indicators(
        df,
        metric_mapping,
        indicator_column=indicator_column,
        dataset_name=dataset_name,
    )

    validate_unique_indicator_observations(
        selected,
        grain_columns=grain,
        indicator_column=indicator_column,
        dataset_name=dataset_name,
    )

    approved_ids = list(
        selected_indicator_ids(
            metric_mapping
        )
    )

    pivoted = (
        selected
        .groupBy(
            *grain
        )
        .pivot(
            indicator_column,
            approved_ids,
        )
        .agg(
            F.first(
                F.col(
                    value_column
                ),
                ignorenulls=False,
            )
        )
    )

    metric_expressions = [
        F.col(
            f"`{indicator_id}`"
        ).cast(
            "double"
        ).alias(
            metric_name
        )
        for indicator_id, metric_name
        in metric_mapping.items()
    ]

    return pivoted.select(
        *[
            F.col(column)
            for column in grain
        ],
        *metric_expressions,
    )


# ============================================================================
# Province-hourly ESIOS energy metrics
# ============================================================================

def prepare_hourly_energy_metrics(
    df: DataFrame,
) -> DataFrame:
    """
    Prepare the approved ESIOS hourly energy metrics at Province Ã— hour.

    Expected input:
        already geographically normalized;
        already temporally aligned according to the Gold ESIOS gap rule.

    Approved transformation:
        metric_mwh = Silver value

    No aggregation of the source value is performed.
    """
    grain_columns = [
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
        "gold_timestamp",
    ]

    return pivot_indicator_metrics(
        df,
        grain_columns=grain_columns,
        metric_mapping=HOURLY_ENERGY_METRICS,
        value_column="value",
        indicator_column="indicator_id",
        dataset_name=(
            "gold_fact_province_hourly "
            "ESIOS energy source"
        ),
    )


# ============================================================================
# Monthly installed-capacity metrics
# ============================================================================

def prepare_installed_capacity_metrics(
    df: DataFrame,
) -> DataFrame:
    """
    Prepare the nine approved installed-capacity metrics at CCAA Ã— month.

    Expected input:
        canonical autonomous-community geography from Silver;
        year_month and gold_month_timestamp already prepared by Gold temporal
        logic.

    Approved transformation:
        installed_capacity_mw = Silver value

    No MW -> MWh conversion.
    No temporal SUM(MW).
    No automatic +1-hour ESIOS gap.
    """
    grain_columns = [
        "year_month",
        "gold_month_timestamp",
        "source_timestamp",
        "autonomous_community_code",
        "autonomous_community_name",
        "esios_geo_id",
    ]

    return pivot_indicator_metrics(
        df,
        grain_columns=grain_columns,
        metric_mapping=INSTALLED_CAPACITY_METRICS,
        value_column="value",
        indicator_column="indicator_id",
        dataset_name=(
            "gold_fact_installed_capacity_monthly "
            "ESIOS source"
        ),
    )


# ============================================================================
# Explicit metric-selection helpers
# ============================================================================

def hourly_energy_metric_names() -> tuple[str, ...]:
    return tuple(
        HOURLY_ENERGY_METRICS.values()
    )


def installed_capacity_metric_names() -> tuple[str, ...]:
    return tuple(
        INSTALLED_CAPACITY_METRICS.values()
    )
