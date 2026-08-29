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
# ============================================================================


# ============================================================================
# Deterministic Gold time keys
# ============================================================================

def add_deterministic_time_key(
    df: DataFrame,
    *,
    time_grain: str,
    timestamp_column: str = "gold_timestamp",
    year_month_column: str = "year_month",
    target_column: str = "time_key",
) -> DataFrame:
    """
    Add the deterministic Gold time identifier.

    Approved Gold grains:

        HOUR
        MONTH

    Hourly keys are generated from:

        HOUR + canonical timestamp

    Monthly keys are generated from:

        MONTH + year_month
    """
    valid_grains = {
        "HOUR",
        "MONTH",
    }

    if time_grain not in valid_grains:
        raise ValueError(
            f"Unsupported Gold time grain: {time_grain}"
        )

    if time_grain == "MONTH":
        if year_month_column not in df.columns:
            raise ValueError(
                "Missing year_month column required "
                "for MONTH time_key."
            )

        temporal_value = F.col(
            year_month_column
        ).cast(
            "string"
        )

    else:
        if timestamp_column not in df.columns:
            raise ValueError(
                "Missing timestamp column required "
                "for HOUR time_key."
            )

        temporal_value = F.date_format(
            F.col(timestamp_column),
            "yyyy-MM-dd'T'HH:mm:ss",
        )

    return df.withColumn(
        target_column,
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(time_grain),
                temporal_value,
            ),
            256,
        ),
    )
