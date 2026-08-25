from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================================
# Approved Gold weather metrics
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
    Fail explicitly when an input DataFrame does not contain the structural
    or meteorological columns required by the approved Gold transformation.
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
    grain_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """
    Validate uniqueness before or after source integration.

    Gold transformations must reach their approved analytical grain before
    joining with another source.
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
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicate_count} duplicated Gold grains."
        )


def validate_non_null_structural_columns(
    df: DataFrame,
    *,
    structural_columns: Iterable[str],
    dataset_name: str,
) -> None:
    """
    Structural Gold grain columns must not be NULL.

    Meteorological metrics themselves may remain NULL when source coverage is
    genuinely absent.
    """
    columns = list(
        structural_columns
    )

    validate_required_columns(
        df,
        columns,
        dataset_name,
    )

    null_condition = None

    for column in columns:
        condition = F.col(
            column
        ).isNull()

        if null_condition is None:
            null_condition = condition
        else:
            null_condition = (
                null_condition
                |
                condition
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
            f"{null_count} rows with NULL structural columns."
        )


# ============================================================================
# Circular means
# ============================================================================

def circular_mean_expression(
    column_name: str,
):
    """
    Spark aggregation expression for a circular mean in degrees.

    Arithmetic AVG must never be used for wind directions.

    Calculation:
        degrees(
            atan2(
                AVG(sin(radians(direction))),
                AVG(cos(radians(direction)))
            )
        )

    The final result is normalized to [0, 360).

    NULL source directions are ignored by AVG in the same way as scalar
    aggregations ignore unavailable observations.
    """
    radians_value = F.radians(
        F.col(
            column_name
        ).cast("double")
    )

    mean_sin = F.avg(
        F.sin(
            radians_value
        )
    )

    mean_cos = F.avg(
        F.cos(
            radians_value
        )
    )

    angle_degrees = F.degrees(
        F.atan2(
            mean_sin,
            mean_cos,
        )
    )

    return F.when(
        mean_sin.isNull()
        |
        mean_cos.isNull(),
        F.lit(None).cast("double"),
    ).otherwise(
        F.pmod(
            angle_degrees
            + F.lit(360.0),
            F.lit(360.0),
        )
    )


# ============================================================================
# AEMET -> Province × hour
# ============================================================================

def prepare_aemet_province_hourly(
    aemet_current_df: DataFrame,
    aemet_stations_df: DataFrame,
) -> DataFrame:
    """
    Aggregate AEMET current observations to Province × hour.

    Approved source mapping:
        ta   -> temperature
        hr   -> humidity
        prec -> precipitation

    AEMET current observations do not carry canonical province columns in
    their persisted Silver schema, so station_id is resolved against the
    Silver AEMET station master, which already carries canonical CNIG
    geography.

    Observations whose station_id cannot be resolved to canonical province
    geography remain preserved upstream in Bronze/Silver but are not eligible
    for the Province × hour Gold product.

    No geographical value is manufactured in Gold.

    Missing analytical coverage may be supplied only by Open-Meteo data
    already ingested into Bronze and normalized in Silver.

    No additional temporal aggregation is applied.
    Only spatial AVG across available stations is performed.
    """
    validate_required_columns(
        aemet_current_df,
        {
            "station_id",
            "observation_timestamp",
            "ta",
            "hr",
            "prec",
        },
        "silver_aemet_current_observations",
    )

    validate_required_columns(
        aemet_stations_df,
        {
            "station_id",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        },
        "silver_aemet_stations",
    )

    station_geography = (
        aemet_stations_df
        .select(
            "station_id",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        )
    )

    validate_unique_grain(
        station_geography,
        grain_columns=[
            "station_id",
        ],
        dataset_name=(
            "silver_aemet_stations geography"
        ),
    )

    enriched = (
        aemet_current_df.alias(
            "observations"
        )
        .join(
            F.broadcast(
                station_geography.alias(
                    "stations"
                )
            ),
            on="station_id",
            how="left",
        )
    )

    # ------------------------------------------------------------------------
    # Gold Province × hour requires canonical province geography.
    #
    # AEMET current observations whose station_id is absent from the
    # validated AEMET station catalogue remain available in Bronze/Silver
    # for traceability and quality analysis.
    #
    # They must not be assigned an invented province in Gold.
    # ------------------------------------------------------------------------

    unmatched_geography = (
        enriched
        .filter(
            F.col(
                "province_code"
            ).isNull()
            |
            F.col(
                "province_name"
            ).isNull()
        )
    )

    unmatched_geography_count = (
        unmatched_geography
        .count()
    )

    unmatched_station_count = (
        unmatched_geography
        .select(
            "station_id"
        )
        .where(
            F.col(
                "station_id"
            ).isNotNull()
        )
        .distinct()
        .count()
    )

    print(
        "AEMET_UNMATCHED_GEOGRAPHY_ROWS = "
        f"{unmatched_geography_count}"
    )

    print(
        "AEMET_UNMATCHED_GEOGRAPHY_STATIONS = "
        f"{unmatched_station_count}"
    )

    # ------------------------------------------------------------------------
    # Only observations with validated canonical geography are eligible for
    # the Province × hour Gold analytical product.
    #
    # This is an analytical eligibility rule, not deletion of source data.
    # ------------------------------------------------------------------------

    geographically_resolved = (
        enriched
        .filter(
            F.col(
                "province_code"
            ).isNotNull()
            &
            F.col(
                "province_name"
            ).isNotNull()
        )
    )

    resolved_geography_count = (
        geographically_resolved
        .count()
    )

    print(
        "AEMET_RESOLVED_GEOGRAPHY_ROWS = "
        f"{resolved_geography_count}"
    )

    result = (
        geographically_resolved
        .groupBy(
            F.col(
                "province_code"
            ),
            F.col(
                "province_name"
            ),
            F.col(
                "autonomous_community_code"
            ),
            F.col(
                "autonomous_community_name"
            ),
            F.col(
                "observation_timestamp"
            ).alias(
                "gold_timestamp"
            ),
        )
        .agg(
            F.avg(
                F.col("ta")
            ).cast(
                "double"
            ).alias(
                "aemet_temperature"
            ),
            F.avg(
                F.col("hr")
            ).cast(
                "double"
            ).alias(
                "aemet_humidity"
            ),
            F.avg(
                F.col("prec")
            ).cast(
                "double"
            ).alias(
                "aemet_precipitation"
            ),
        )
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name=(
            "AEMET Province × hour"
        ),
    )

    return result


# ============================================================================
# Open-Meteo hourly -> Province × hour
# ============================================================================

def prepare_open_meteo_province_hourly(
    open_meteo_hourly_df: DataFrame,
) -> DataFrame:
    """
    Aggregate Open-Meteo hourly observations to Province × hour.

    Approved uses:
        temperature_2m
            -> fallback temperature

        relative_humidity_2m
            -> fallback humidity

        precipitation
            -> fallback precipitation

        shortwave_radiation
            -> solar_radiation

        direct_normal_irradiance
            -> direct_normal_irradiance

    No additional temporal aggregation is performed.
    """
    validate_required_columns(
        open_meteo_hourly_df,
        {
            "observation_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "shortwave_radiation",
            "direct_normal_irradiance",
        },
        "silver_open_meteo_hourly",
    )

    result = (
        open_meteo_hourly_df
        .groupBy(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            F.col(
                "observation_timestamp"
            ).alias(
                "gold_timestamp"
            ),
        )
        .agg(
            F.avg(
                F.col(
                    "temperature_2m"
                )
            ).cast(
                "double"
            ).alias(
                "open_meteo_temperature"
            ),
            F.avg(
                F.col(
                    "relative_humidity_2m"
                )
            ).cast(
                "double"
            ).alias(
                "open_meteo_humidity"
            ),
            F.avg(
                F.col(
                    "precipitation"
                )
            ).cast(
                "double"
            ).alias(
                "open_meteo_precipitation"
            ),
            F.avg(
                F.col(
                    "shortwave_radiation"
                )
            ).cast(
                "double"
            ).alias(
                "solar_radiation"
            ),
            F.avg(
                F.col(
                    "direct_normal_irradiance"
                )
            ).cast(
                "double"
            ).alias(
                "direct_normal_irradiance"
            ),
        )
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name=(
            "Open-Meteo hourly Province × hour"
        ),
    )

    return result


# ============================================================================
# Open-Meteo 15 min wind -> Point × hour
# ============================================================================

def prepare_open_meteo_wind_point_hourly(
    open_meteo_15min_df: DataFrame,
) -> DataFrame:
    """
    First approved wind aggregation stage:

        four 15-minute observations
        -> one hourly value per point.

    Wind speeds:
        arithmetic AVG.

    Wind directions:
        circular mean.
    """
    validate_required_columns(
        open_meteo_15min_df,
        {
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
        },
        "silver_open_meteo_15min",
    )

    result = (
        open_meteo_15min_df
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
            "station_id",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "gold_timestamp",
        )
        .agg(
            F.avg(
                F.col(
                    "wind_speed_80m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_80m"
            ),
            circular_mean_expression(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),
            F.avg(
                F.col(
                    "wind_speed_120m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_120m"
            ),
            circular_mean_expression(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),
        )
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "station_id",
            "gold_timestamp",
        ],
        dataset_name=(
            "Open-Meteo wind Point × hour"
        ),
    )

    return result


# ============================================================================
# Open-Meteo wind Point × hour -> Province × hour
# ============================================================================

def prepare_open_meteo_wind_province_hourly(
    open_meteo_15min_df: DataFrame,
) -> DataFrame:
    """
    Complete approved 15-minute wind transformation:

        15 min
        -> hourly per point
        -> Province × hour.

    Speeds:
        AVG points.

    Directions:
        circular mean across point-level hourly directions.
    """
    point_hourly = (
        prepare_open_meteo_wind_point_hourly(
            open_meteo_15min_df
        )
    )

    result = (
        point_hourly
        .groupBy(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
            "gold_timestamp",
        )
        .agg(
            F.avg(
                F.col(
                    "wind_speed_80m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_80m"
            ),
            circular_mean_expression(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),
            F.avg(
                F.col(
                    "wind_speed_120m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_120m"
            ),
            circular_mean_expression(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),
        )
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name=(
            "Open-Meteo wind Province × hour"
        ),
    )

    return result


# ============================================================================
# Province-hourly weather integration
# ============================================================================

def prepare_province_hourly_weather(
    aemet_current_df: DataFrame,
    aemet_stations_df: DataFrame,
    open_meteo_hourly_df: DataFrame,
    open_meteo_15min_df: DataFrame,
) -> DataFrame:
    """
    Build the complete approved meteorological product at Province × hour.

    Rules:
        temperature:
            AEMET first, Open-Meteo fallback.

        humidity:
            AEMET first, Open-Meteo fallback.

        precipitation:
            AEMET first, Open-Meteo fallback.

        wind 80/120 m:
            Open-Meteo 15-minute only.

        solar_radiation:
            Open-Meteo hourly shortwave_radiation.

        direct_normal_irradiance:
            Open-Meteo hourly DNI.

    Fallback is applied independently per metric.

    NULL is never automatically converted to zero.
    """
    aemet = (
        prepare_aemet_province_hourly(
            aemet_current_df,
            aemet_stations_df,
        )
        .alias(
            "aemet"
        )
    )

    open_meteo = (
        prepare_open_meteo_province_hourly(
            open_meteo_hourly_df
        )
        .alias(
            "open_meteo"
        )
    )

    wind = (
        prepare_open_meteo_wind_province_hourly(
            open_meteo_15min_df
        )
        .alias(
            "wind"
        )
    )

    # ------------------------------------------------------------------------
    # First combine Open-Meteo hourly and Open-Meteo wind.
    #
    # Both are already Province × hour before this join.
    # ------------------------------------------------------------------------

    open_meteo_complete = (
        open_meteo
        .join(
            wind,
            on=[
                "province_code",
                "province_name",
                "autonomous_community_code",
                "autonomous_community_name",
                "gold_timestamp",
            ],
            how="full_outer",
        )
        .alias(
            "om"
        )
    )

    validate_unique_grain(
        open_meteo_complete,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name=(
            "Combined Open-Meteo Province × hour"
        ),
    )

    # ------------------------------------------------------------------------
    # Full outer join because AEMET current coverage is intentionally shorter
    # than the reproducible Open-Meteo historical coverage.
    #
    # Join only after both sources have reached Province × hour.
    # ------------------------------------------------------------------------

    joined = (
        open_meteo_complete
        .join(
            aemet,
            on=[
                "province_code",
                "province_name",
                "autonomous_community_code",
                "autonomous_community_name",
                "gold_timestamp",
            ],
            how="full_outer",
        )
    )

    result = (
        joined
        .select(
            "gold_timestamp",
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",

            F.coalesce(
                F.col(
                    "aemet_temperature"
                ),
                F.col(
                    "open_meteo_temperature"
                ),
            ).cast(
                "double"
            ).alias(
                "temperature"
            ),

            F.coalesce(
                F.col(
                    "aemet_humidity"
                ),
                F.col(
                    "open_meteo_humidity"
                ),
            ).cast(
                "double"
            ).alias(
                "humidity"
            ),

            F.coalesce(
                F.col(
                    "aemet_precipitation"
                ),
                F.col(
                    "open_meteo_precipitation"
                ),
            ).cast(
                "double"
            ).alias(
                "precipitation"
            ),

            F.col(
                "wind_speed_80m"
            ).cast(
                "double"
            ).alias(
                "wind_speed_80m"
            ),

            F.col(
                "wind_direction_80m"
            ).cast(
                "double"
            ).alias(
                "wind_direction_80m"
            ),

            F.col(
                "wind_speed_120m"
            ).cast(
                "double"
            ).alias(
                "wind_speed_120m"
            ),

            F.col(
                "wind_direction_120m"
            ).cast(
                "double"
            ).alias(
                "wind_direction_120m"
            ),

            F.col(
                "solar_radiation"
            ).cast(
                "double"
            ).alias(
                "solar_radiation"
            ),

            F.col(
                "direct_normal_irradiance"
            ).cast(
                "double"
            ).alias(
                "direct_normal_irradiance"
            ),

            F.when(
                F.col(
                    "aemet_temperature"
                ).isNotNull(),
                F.lit(
                    "AEMET"
                ),
            )
            .when(
                F.col(
                    "open_meteo_temperature"
                ).isNotNull(),
                F.lit(
                    "OPEN_METEO"
                ),
            )
            .otherwise(
                F.lit(
                    None
                ).cast(
                    "string"
                )
            )
            .alias(
                "temperature_source"
            ),

            F.when(
                F.col(
                    "aemet_humidity"
                ).isNotNull(),
                F.lit(
                    "AEMET"
                ),
            )
            .when(
                F.col(
                    "open_meteo_humidity"
                ).isNotNull(),
                F.lit(
                    "OPEN_METEO"
                ),
            )
            .otherwise(
                F.lit(
                    None
                ).cast(
                    "string"
                )
            )
            .alias(
                "humidity_source"
            ),

            F.when(
                F.col(
                    "aemet_precipitation"
                ).isNotNull(),
                F.lit(
                    "AEMET"
                ),
            )
            .when(
                F.col(
                    "open_meteo_precipitation"
                ).isNotNull(),
                F.lit(
                    "OPEN_METEO"
                ),
            )
            .otherwise(
                F.lit(
                    None
                ).cast(
                    "string"
                )
            )
            .alias(
                "precipitation_source"
            ),
        )
    )

    validate_non_null_structural_columns(
        result,
        structural_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name=(
            "Gold Province × hour weather"
        ),
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name=(
            "Gold Province × hour weather"
        ),
    )

    return result


# ============================================================================
# Open-Meteo 15 min -> Province × 15 min
# ============================================================================

def prepare_open_meteo_province_15min(
    open_meteo_15min_df: DataFrame,
) -> DataFrame:
    """
    First spatial stage for the approved national 15-minute weather product.

        points
        -> Province × 15 min

    There is no temporal aggregation.

    Scalars:
        arithmetic AVG.

    Directions:
        circular mean.
    """
    validate_required_columns(
        open_meteo_15min_df,
        {
            "observation_timestamp",
            "province_code",
            "province_name",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_80m",
            "wind_direction_80m",
            "wind_speed_120m",
            "wind_direction_120m",
            "shortwave_radiation",
            "direct_normal_irradiance",
        },
        "silver_open_meteo_15min",
    )

    result = (
        open_meteo_15min_df
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
            F.avg(
                F.col(
                    "temperature_2m"
                )
            ).cast(
                "double"
            ).alias(
                "temperature"
            ),

            F.avg(
                F.col(
                    "relative_humidity_2m"
                )
            ).cast(
                "double"
            ).alias(
                "humidity"
            ),

            F.avg(
                F.col(
                    "precipitation"
                )
            ).cast(
                "double"
            ).alias(
                "precipitation"
            ),

            F.avg(
                F.col(
                    "wind_speed_80m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_80m"
            ),

            circular_mean_expression(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),

            F.avg(
                F.col(
                    "wind_speed_120m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_120m"
            ),

            circular_mean_expression(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),

            F.avg(
                F.col(
                    "shortwave_radiation"
                )
            ).cast(
                "double"
            ).alias(
                "solar_radiation"
            ),

            F.avg(
                F.col(
                    "direct_normal_irradiance"
                )
            ).cast(
                "double"
            ).alias(
                "direct_normal_irradiance"
            ),
        )
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "province_code",
            "gold_timestamp",
        ],
        dataset_name=(
            "Open-Meteo Province × 15 min"
        ),
    )

    return result


# ============================================================================
# Province × 15 min -> Spain × 15 min
# ============================================================================

def prepare_country_15min_weather(
    open_meteo_15min_df: DataFrame,
    *,
    geography_key: str,
) -> DataFrame:
    """
    Build the approved national meteorological product:

        Open-Meteo point
        -> Province × 15 min
        -> Spain × 15 min.

    Scalars:
        AVG of province-level averages.

    Directions:
        circular mean of province-level circular means.

    Spain weather is COUNTRY geography.
    Peninsula is not manufactured from meteorological data.

    geography_key is supplied by the Gold geography implementation so that
    weather.py does not invent the literal serialization of the deterministic
    geography key.
    """
    province = (
        prepare_open_meteo_province_15min(
            open_meteo_15min_df
        )
    )

    result = (
        province
        .groupBy(
            "gold_timestamp"
        )
        .agg(
            F.avg(
                F.col(
                    "temperature"
                )
            ).cast(
                "double"
            ).alias(
                "temperature"
            ),

            F.avg(
                F.col(
                    "humidity"
                )
            ).cast(
                "double"
            ).alias(
                "humidity"
            ),

            F.avg(
                F.col(
                    "precipitation"
                )
            ).cast(
                "double"
            ).alias(
                "precipitation"
            ),

            F.avg(
                F.col(
                    "wind_speed_80m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_80m"
            ),

            circular_mean_expression(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),

            F.avg(
                F.col(
                    "wind_speed_120m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_120m"
            ),

            circular_mean_expression(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),

            F.avg(
                F.col(
                    "solar_radiation"
                )
            ).cast(
                "double"
            ).alias(
                "solar_radiation"
            ),

            F.avg(
                F.col(
                    "direct_normal_irradiance"
                )
            ).cast(
                "double"
            ).alias(
                "direct_normal_irradiance"
            ),
        )
        .withColumn(
            "geography_key",
            F.lit(
                geography_key
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
        .select(
            "gold_timestamp",
            "geography_key",
            "geography_level",
            "geography_name",
            *COUNTRY_15MIN_WEATHER_METRICS,
        )
    )

    validate_non_null_structural_columns(
        result,
        structural_columns=[
            "geography_key",
            "gold_timestamp",
        ],
        dataset_name=(
            "Gold Spain × 15 min weather"
        ),
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "geography_key",
            "gold_timestamp",
        ],
        dataset_name=(
            "Gold Spain × 15 min weather"
        ),
    )

    return result

def prepare_peninsula_15min_weather(
    open_meteo_15min_df: DataFrame,
    *,
    geography_key: str,
    excluded_province_codes: Iterable[str],
) -> DataFrame:
    """
    Build the approved Peninsula × 15 min meteorological product.

    Source flow:
        Open-Meteo points
        -> Province × 15 min
        -> validated peninsular provinces
        -> Peninsula × 15 min

    The Peninsula scope is defined externally through Gold configuration.

    Spain is never converted into Peninsula.
    The aggregation starts from real province-level meteorological data.

    Scalars:
        AVG of province-level averages.

    Directions:
        circular mean of province-level circular means.
    """
    province = (
        prepare_open_meteo_province_15min(
            open_meteo_15min_df
        )
    )

    excluded_codes = list(
        excluded_province_codes
    )

    peninsula = (
        province
        .filter(
            ~F.col(
                "province_code"
            ).isin(
                excluded_codes
            )
        )
    )

    result = (
        peninsula
        .groupBy(
            "gold_timestamp"
        )
        .agg(
            F.avg(
                F.col(
                    "temperature"
                )
            ).cast(
                "double"
            ).alias(
                "temperature"
            ),

            F.avg(
                F.col(
                    "humidity"
                )
            ).cast(
                "double"
            ).alias(
                "humidity"
            ),

            F.avg(
                F.col(
                    "precipitation"
                )
            ).cast(
                "double"
            ).alias(
                "precipitation"
            ),

            F.avg(
                F.col(
                    "wind_speed_80m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_80m"
            ),

            circular_mean_expression(
                "wind_direction_80m"
            ).alias(
                "wind_direction_80m"
            ),

            F.avg(
                F.col(
                    "wind_speed_120m"
                )
            ).cast(
                "double"
            ).alias(
                "wind_speed_120m"
            ),

            circular_mean_expression(
                "wind_direction_120m"
            ).alias(
                "wind_direction_120m"
            ),

            F.avg(
                F.col(
                    "solar_radiation"
                )
            ).cast(
                "double"
            ).alias(
                "solar_radiation"
            ),

            F.avg(
                F.col(
                    "direct_normal_irradiance"
                )
            ).cast(
                "double"
            ).alias(
                "direct_normal_irradiance"
            ),
        )
        .withColumn(
            "geography_key",
            F.lit(
                geography_key
            ),
        )
        .withColumn(
            "geography_level",
            F.lit(
                "PENINSULA"
            ),
        )
        .withColumn(
            "geography_name",
            F.lit(
                "Península"
            ),
        )
        .select(
            "gold_timestamp",
            "geography_key",
            "geography_level",
            "geography_name",
            *COUNTRY_15MIN_WEATHER_METRICS,
        )
    )

    validate_non_null_structural_columns(
        result,
        structural_columns=[
            "geography_key",
            "gold_timestamp",
        ],
        dataset_name=(
            "Gold Peninsula × 15 min weather"
        ),
    )

    validate_unique_grain(
        result,
        grain_columns=[
            "geography_key",
            "gold_timestamp",
        ],
        dataset_name=(
            "Gold Peninsula × 15 min weather"
        ),
    )

    return result