from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================================
# ESIOS temporal alignment
# ============================================================================

def apply_esios_time_gap(
    df: DataFrame,
    gap_hours: int,
    *,
    source_timestamp_column: str = "observation_timestamp",
    target_timestamp_column: str = "gold_timestamp",
) -> DataFrame:
    """
    Apply the approved configurable ESIOS -> Gold temporal alignment.

    gold_timestamp =
        observation_timestamp + configurable_gap

    The gap must be provided by the caller from Gold configuration.
    It is deliberately not hardcoded here.
    """
    if source_timestamp_column not in df.columns:
        raise ValueError(
            f"Missing timestamp column: {source_timestamp_column}"
        )

    if isinstance(gap_hours, bool) or not isinstance(gap_hours, int):
        raise ValueError(
            "gap_hours must be an integer."
        )

    return df.withColumn(
        target_timestamp_column,
        F.col(source_timestamp_column)
        + F.expr(f"INTERVAL {gap_hours} HOURS"),
    )


# ============================================================================
# Generic temporal helpers
# ============================================================================

def add_hour_timestamp(
    df: DataFrame,
    *,
    timestamp_column: str = "observation_timestamp",
    target_column: str = "gold_timestamp",
) -> DataFrame:
    """
    Truncate a timestamp to its hour.

    This helper does not fill missing timestamps and does not fabricate rows.
    """
    if timestamp_column not in df.columns:
        raise ValueError(
            f"Missing timestamp column: {timestamp_column}"
        )

    return df.withColumn(
        target_column,
        F.date_trunc(
            "hour",
            F.col(timestamp_column),
        ),
    )


def add_15min_timestamp(
    df: DataFrame,
    *,
    timestamp_column: str = "gold_timestamp",
    target_column: str = "gold_timestamp_15min",
) -> DataFrame:
    """
    Assign an existing timestamp to its natural 15-minute bucket.

    This is used only when aggregating real 5-minute ESIOS observations
    into a 15-minute Gold interval.
    """
    if timestamp_column not in df.columns:
        raise ValueError(
            f"Missing timestamp column: {timestamp_column}"
        )

    seconds = F.unix_timestamp(
        F.col(timestamp_column)
    )

    bucket_seconds = (
        F.floor(seconds / F.lit(900))
        * F.lit(900)
    )

    return df.withColumn(
        target_column,
        F.to_timestamp(
            F.from_unixtime(bucket_seconds)
        ),
    )


# ============================================================================
# Circular mean
# ============================================================================

def circular_mean_degrees(
    column_name: str,
):
    """
    Return a Spark aggregation expression for a circular mean in degrees.

    The result is normalized to the interval [0, 360).
    """
    radians = F.radians(
        F.col(column_name)
    )

    mean_sin = F.avg(
        F.sin(radians)
    )

    mean_cos = F.avg(
        F.cos(radians)
    )

    degrees = F.degrees(
        F.atan2(
            mean_sin,
            mean_cos,
        )
    )

    return F.pmod(
        degrees + F.lit(360.0),
        F.lit(360.0),
    )


# ============================================================================
# Open-Meteo 15 min -> hourly wind by point
# ============================================================================

def aggregate_open_meteo_wind_to_hourly_point(
    df: DataFrame,
) -> DataFrame:
    """
    Aggregate Open-Meteo 15-minute wind data to one hourly observation
    per point/station.

    Approved rules:

    wind speed:
        15 min -> hour = arithmetic AVG

    wind direction:
        15 min -> hour = circular mean

    No spatial aggregation is performed here.
    """
    required_columns = {
        "station_id",
        "observation_timestamp",
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
        "wind_speed_80m",
        "wind_direction_80m",
        "wind_speed_120m",
        "wind_direction_120m",
    }

    missing = sorted(
        required_columns.difference(df.columns)
    )

    if missing:
        raise ValueError(
            "Missing required Open-Meteo columns: "
            f"{missing}"
        )

    hourly = add_hour_timestamp(
        df,
        timestamp_column="observation_timestamp",
        target_column="gold_timestamp",
    )

    return (
        hourly
        .groupBy(
            "station_id",
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
            F.count(
                F.lit(1)
            ).alias(
                "source_interval_count"
            ),
        )
    )


# ============================================================================
# ESIOS 5-minute power -> interval energy
# ============================================================================

def add_esios_5min_energy(
    df: DataFrame,
    metric_mapping: Mapping[int, str],
) -> DataFrame:
    """
    Convert each real ESIOS 5-minute power observation into interval energy.

    For every observation:

        energy_mwh_5min = power_mw * (5 / 60)

    The original sign is preserved.
    """
    required_columns = {
        "indicator_id",
        "value",
        "gold_timestamp",
    }

    missing = sorted(
        required_columns.difference(df.columns)
    )

    if missing:
        raise ValueError(
            "Missing required ESIOS columns: "
            f"{missing}"
        )

    indicator_ids = list(
        metric_mapping.keys()
    )

    filtered = df.filter(
        F.col("indicator_id").isin(
            indicator_ids
        )
    )

    return (
        filtered
        .withColumn(
            "power_mw",
            F.col("value"),
        )
        .withColumn(
            "energy_mwh_5min",
            F.col("power_mw")
            * F.lit(5.0 / 60.0),
        )
    )


# ============================================================================
# ESIOS 5 min -> 15 min energy
# ============================================================================

def aggregate_esios_energy_5min_to_15min(
    df: DataFrame,
) -> DataFrame:
    """
    Aggregate real 5-minute ESIOS interval energy to 15 minutes.

    Approved rule:

        energy_mwh_15min =
            SUM(three energy_mwh_5min intervals)

    Power MW is never summed.
    """
    required_columns = {
        "indicator_id",
        "esios_geo_id",
        "esios_geo_name",
        "gold_timestamp",
        "energy_mwh_5min",
    }

    missing = sorted(
        required_columns.difference(df.columns)
    )

    if missing:
        raise ValueError(
            "Missing required ESIOS columns: "
            f"{missing}"
        )

    bucketed = add_15min_timestamp(
        df,
        timestamp_column="gold_timestamp",
        target_column="gold_timestamp_15min",
    )

    return (
        bucketed
        .groupBy(
            "indicator_id",
            "esios_geo_id",
            "esios_geo_name",
            "gold_timestamp_15min",
        )
        .agg(
            F.sum(
                "energy_mwh_5min"
            ).alias(
                "energy_mwh_15min"
            ),
            F.count(
                F.lit(1)
            ).alias(
                "source_interval_count"
            ),
        )
        .withColumnRenamed(
            "gold_timestamp_15min",
            "gold_timestamp",
        )
    )