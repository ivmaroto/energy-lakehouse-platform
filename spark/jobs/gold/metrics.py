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
# Province × hour
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
# Autonomous Community × month
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
# Spain / Peninsula × 5 minutes
#
# Silver source:
#     silver_esios_power_5min
#
# Original measure:
#     power_mw = value
#
# Derived interval energy:
#     energy_mwh_5min = power_mw * (5 / 60)
#
# Original signs are preserved.
# ============================================================================

HIGH_FREQUENCY_POWER_METRICS: dict[int, str] = {
    1293: "real_demand_mw",
    2038: "wind_generation_power_mw",
    2039: "nuclear_generation_power_mw",
    2040: "coal_generation_power_mw",
    2041: "combined_cycle_generation_power_mw",
    2042: "hydraulic_generation_power_mw",
    2044: "solar_photovoltaic_generation_power_mw",
    2045: "solar_thermal_generation_power_mw",
    2046: "renewable_thermal_generation_power_mw",
    2051: "cogeneration_waste_generation_power_mw",
    2065: "pumping_consumption_power_mw",
}

HIGH_FREQUENCY_ENERGY_5MIN_METRICS: dict[int, str] = {
    1293: "real_demand_energy_mwh_5min",
    2038: "wind_generation_energy_mwh_5min",
    2039: "nuclear_generation_energy_mwh_5min",
    2040: "coal_generation_energy_mwh_5min",
    2041: "combined_cycle_generation_energy_mwh_5min",
    2042: "hydraulic_generation_energy_mwh_5min",
    2044: "solar_photovoltaic_generation_energy_mwh_5min",
    2045: "solar_thermal_generation_energy_mwh_5min",
    2046: "renewable_thermal_generation_energy_mwh_5min",
    2051: "cogeneration_waste_generation_energy_mwh_5min",
    2065: "pumping_consumption_energy_mwh_5min",
}


# ============================================================================
# Spain / Peninsula × 15 minutes
#
# Energy is constructed in temporal.py from three real 5-minute interval
# energies.
#
# Never:
#     SUM(power_mw)
#
# Correct rule:
#     energy_mwh_5min = power_mw * 5 / 60
#     energy_mwh_15min = SUM(three energy_mwh_5min intervals)
# ============================================================================

HIGH_FREQUENCY_ENERGY_15MIN_METRICS: dict[int, str] = {
    1293: "real_demand_energy_mwh_15min",
    2038: "wind_generation_energy_mwh_15min",
    2039: "nuclear_generation_energy_mwh_15min",
    2040: "coal_generation_energy_mwh_15min",
    2041: "combined_cycle_generation_energy_mwh_15min",
    2042: "hydraulic_generation_energy_mwh_15min",
    2044: "solar_photovoltaic_generation_energy_mwh_15min",
    2045: "solar_thermal_generation_energy_mwh_15min",
    2046: "renewable_thermal_generation_energy_mwh_15min",
    2051: "cogeneration_waste_generation_energy_mwh_15min",
    2065: "pumping_consumption_energy_mwh_15min",
}

HIGH_FREQUENCY_EXCLUDED_INDICATORS: frozenset[int] = frozenset(
    {
        10004,
    }
)


# ============================================================================
# Approved high-frequency geographical scopes
#
# Validated Gold rule:
#
#     1293
#         -> Peninsula
#
#     selected 2038..2065 indicators
#         -> Spain
#
# Spain and Peninsula must remain distinct.
# ============================================================================

PENINSULA_HIGH_FREQUENCY_INDICATORS: frozenset[int] = frozenset(
    {
        1293,
    }
)

SPAIN_HIGH_FREQUENCY_INDICATORS: frozenset[int] = frozenset(
    {
        2038,
        2039,
        2040,
        2041,
        2042,
        2044,
        2045,
        2046,
        2051,
        2065,
    }
)


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

COUNTRY_15MIN_WEATHER_METRICS: tuple[str, ...] = (
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
    Prepare the approved ESIOS hourly energy metrics at Province × hour.

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
    Prepare the nine approved installed-capacity metrics at CCAA × month.

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
# Five-minute high-frequency metrics
# ============================================================================

def add_energy_mwh_5min(
    df: DataFrame,
    *,
    power_column: str = "value",
    output_column: str = "energy_mwh_5min",
) -> DataFrame:
    """
    Convert a real 5-minute ESIOS power observation to interval energy.

    Approved rule:
        energy_mwh_5min = power_mw * (5 / 60)

    Equivalent:
        energy_mwh_5min = power_mw / 12

    Original signs are preserved.
    NULL remains NULL.
    """
    validate_required_columns(
        df,
        {
            power_column,
        },
        "ESIOS 5-minute energy conversion",
    )

    return df.withColumn(
        output_column,
        (
            F.col(
                power_column
            ).cast("double")
            * F.lit(5.0 / 60.0)
        ),
    )


def prepare_country_5min_metrics(
    df: DataFrame,
) -> DataFrame:
    """
    Prepare approved high-frequency ESIOS metrics at their real 5-minute
    geography × timestamp grain.

    Expected input:
        gold_timestamp already aligned;
        geography_key / geography_level / geography_name already resolved;
        Spain and Peninsula already kept distinct.

    Output contains:
        11 original power metrics in MW;
        11 derived interval-energy metrics in MWh.
    """
    grain_columns = [
        "gold_timestamp",
        "geography_key",
        "geography_level",
        "geography_name",
        "esios_geo_id",
    ]

    validate_required_columns(
        df,
        [
            *grain_columns,
            "indicator_id",
            "value",
        ],
        "gold_fact_country_5min ESIOS source",
    )

    selected = select_approved_indicators(
        df,
        HIGH_FREQUENCY_POWER_METRICS,
        indicator_column="indicator_id",
        dataset_name=(
            "gold_fact_country_5min ESIOS source"
        ),
    )

    selected = add_energy_mwh_5min(
        selected,
        power_column="value",
        output_column="_energy_mwh_5min",
    )

    validate_unique_indicator_observations(
        selected,
        grain_columns=grain_columns,
        indicator_column="indicator_id",
        dataset_name=(
            "gold_fact_country_5min ESIOS source"
        ),
    )

    approved_ids = list(
        selected_indicator_ids(
            HIGH_FREQUENCY_POWER_METRICS
        )
    )

    power_pivot = (
        selected
        .groupBy(
            *grain_columns
        )
        .pivot(
            "indicator_id",
            approved_ids,
        )
        .agg(
            F.first(
                F.col("value"),
                ignorenulls=False,
            )
        )
    )

    power_expressions = [
        F.col(
            f"`{indicator_id}`"
        ).cast(
            "double"
        ).alias(
            metric_name
        )
        for indicator_id, metric_name
        in HIGH_FREQUENCY_POWER_METRICS.items()
    ]

    power_pivot = power_pivot.select(
        *[
            F.col(column)
            for column in grain_columns
        ],
        *power_expressions,
    )

    energy_pivot = (
        selected
        .groupBy(
            *grain_columns
        )
        .pivot(
            "indicator_id",
            approved_ids,
        )
        .agg(
            F.first(
                F.col("_energy_mwh_5min"),
                ignorenulls=False,
            )
        )
    )

    energy_expressions = [
        F.col(
            f"`{indicator_id}`"
        ).cast(
            "double"
        ).alias(
            HIGH_FREQUENCY_ENERGY_5MIN_METRICS[
                indicator_id
            ]
        )
        for indicator_id
        in approved_ids
    ]

    energy_pivot = energy_pivot.select(
        *[
            F.col(column)
            for column in grain_columns
        ],
        *energy_expressions,
    )

    return power_pivot.join(
        energy_pivot,
        on=grain_columns,
        how="inner",
    )


# ============================================================================
# Fifteen-minute high-frequency energy metric pivot
# ============================================================================

def prepare_country_15min_energy_metrics(
    df: DataFrame,
) -> DataFrame:
    """
    Pivot already aggregated 15-minute interval energies.

    IMPORTANT:
        This function does NOT construct the 15-minute interval.

        temporal.py must previously apply the approved transformation:

            power_mw
                -> energy_mwh_5min
                -> SUM of three real 5-minute interval energies
                -> energy_mwh_15min

        SUM(power_mw) is prohibited.

    Expected long-format value column:
        energy_mwh_15min
    """
    grain_columns = [
        "gold_timestamp",
        "geography_key",
        "geography_level",
        "geography_name",
    ]

    return pivot_indicator_metrics(
        df,
        grain_columns=grain_columns,
        metric_mapping=HIGH_FREQUENCY_ENERGY_15MIN_METRICS,
        value_column="energy_mwh_15min",
        indicator_column="indicator_id",
        dataset_name=(
            "gold_fact_country_15min "
            "ESIOS energy source"
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


def country_5min_power_metric_names() -> tuple[str, ...]:
    return tuple(
        HIGH_FREQUENCY_POWER_METRICS.values()
    )


def country_5min_energy_metric_names() -> tuple[str, ...]:
    return tuple(
        HIGH_FREQUENCY_ENERGY_5MIN_METRICS.values()
    )


def country_15min_energy_metric_names() -> tuple[str, ...]:
    return tuple(
        HIGH_FREQUENCY_ENERGY_15MIN_METRICS.values()
    )