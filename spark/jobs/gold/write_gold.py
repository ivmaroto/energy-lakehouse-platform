from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold.common import (
    TABLE_GOLD_DIM_GEOGRAPHY,
    TABLE_GOLD_DIM_TIME,
    TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY,
    TABLE_GOLD_FACT_PROVINCE_HOURLY,
    TABLE_SILVER_AEMET_CURRENT,
    TABLE_SILVER_AEMET_STATIONS,
    TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES,
    TABLE_SILVER_CNIG_PROVINCES,
    TABLE_SILVER_ESIOS_ENERGY_HOURLY,
    TABLE_SILVER_ESIOS_INSTALLED_CAPACITY_MONTHLY,
    TABLE_SILVER_OPEN_METEO_15MIN,
    TABLE_SILVER_OPEN_METEO_HOURLY,
    get_esios_time_gap_hours,
    read_silver_table,
    validate_table_exists,
)

from gold.geography import (
    add_deterministic_geography_key,
)

from gold.metrics import (
    prepare_hourly_energy_metrics,
    prepare_installed_capacity_metrics,
)

from gold.province_hourly_integration import (
    integrate_province_hourly_weather_energy,
)

from gold.temporal import (
    add_deterministic_time_key,
    apply_esios_time_gap,
)

from gold.weather import (
    prepare_province_hourly_weather,
)


# ============================================================================
# Gold natural keys
# ============================================================================

GOLD_NATURAL_KEYS = {
    TABLE_GOLD_FACT_PROVINCE_HOURLY: [
        "geography_key",
        "gold_timestamp",
    ],
    TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY: [
        "geography_key",
        "year_month",
    ],
    TABLE_GOLD_DIM_TIME: [
        "time_key",
    ],
    TABLE_GOLD_DIM_GEOGRAPHY: [
        "geography_key",
    ],
}


# ============================================================================
# Gold technical metadata
# ============================================================================

GOLD_CREATED_AT_COLUMN = (
    "gold_created_at"
)


def add_gold_created_at(
    df: DataFrame,
) -> DataFrame:
    """
    Add the Gold row-creation timestamp.

    During MERGE:

        new row
            -> source.gold_created_at is inserted

        existing row
            -> target.gold_created_at is preserved

    Therefore reprocessing the same logical Gold row does not alter its
    original creation timestamp.
    """
    return df.withColumn(
        GOLD_CREATED_AT_COLUMN,
        F.current_timestamp(),
    )


# ============================================================================
# Gold fact builders
# ============================================================================

def build_gold_fact_province_hourly(
    spark: SparkSession,
) -> DataFrame:
    """
    Build the physical Gold Province x hour fact from validated Silver data.

    Pipeline:

        AEMET current observations
        + AEMET station geography
        + Open-Meteo hourly
        + Open-Meteo 15-minute
            -> Province x hour meteorology

        ESIOS hourly energy
            -> configurable temporal alignment
            -> approved hourly energy metrics

        weather + energy
            -> full Province x hour integration
            -> deterministic province geography_key
            -> gold_created_at

    Natural grain:

        geography_key + gold_timestamp
    """

    # ========================================================================
    # Read real persisted Silver
    # ========================================================================

    aemet_current = (
        read_silver_table(
            spark=spark,
            table_name=TABLE_SILVER_AEMET_CURRENT,
        )
    )

    aemet_stations = (
        read_silver_table(
            spark=spark,
            table_name=TABLE_SILVER_AEMET_STATIONS,
        )
    )

    open_meteo_hourly = (
        read_silver_table(
            spark=spark,
            table_name=TABLE_SILVER_OPEN_METEO_HOURLY,
        )
    )

    open_meteo_15min = (
        read_silver_table(
            spark=spark,
            table_name=TABLE_SILVER_OPEN_METEO_15MIN,
        )
    )

    esios_energy_hourly = (
        read_silver_table(
            spark=spark,
            table_name=TABLE_SILVER_ESIOS_ENERGY_HOURLY,
        )
    )

    # ========================================================================
    # Province x hour meteorology
    # ========================================================================

    weather = (
        prepare_province_hourly_weather(
            aemet_current,
            aemet_stations,
            open_meteo_hourly,
            open_meteo_15min,
        )
    )

    # ========================================================================
    # Province x hour energy
    # ========================================================================

    esios_time_gap_hours = (
        get_esios_time_gap_hours()
    )

    esios_temporally_aligned = (
        apply_esios_time_gap(
            esios_energy_hourly,
            gap_hours=esios_time_gap_hours,
        )
    )

    energy = (
        prepare_hourly_energy_metrics(
            esios_temporally_aligned
        )
    )

    # ========================================================================
    # Weather <-> energy integration
    # ========================================================================

    result = (
        integrate_province_hourly_weather_energy(
            weather,
            energy,
        )
    )

    # ========================================================================
    # Gold deterministic geography identifier
    # ========================================================================

    result = (
        add_deterministic_geography_key(
            result,
            geography_level="PROVINCE",
            geography_code_column="province_code",
        )
    )

    result = (
        add_deterministic_time_key(
            result,
            time_grain="HOUR",
        )
    )

    # ========================================================================
    # Gold technical metadata
    # ========================================================================

    result = (
        add_gold_created_at(
            result
        )
    )

    return result


# ============================================================================
# Gold time dimension
# ============================================================================

def build_gold_dim_time(
    fact_province_hourly: DataFrame,
    fact_installed_capacity_monthly: DataFrame,
) -> DataFrame:
    """
    Build the Gold time dimension from the real temporal grains present in the two Gold facts.

    Supported grains:

        HOUR
        MONTH

    day_of_week follows ISO analytical convention:

        Monday    = 1
        Tuesday   = 2
        Wednesday = 3
        Thursday  = 4
        Friday    = 5
        Saturday  = 6
        Sunday    = 7

    Monthly rows are represented only by year_month.

    They do not fabricate a Gold timestamp, date, day, day_of_week,hour or minute.
    """

    # ========================================================================
    # Hour
    # ========================================================================

    hourly = (
        fact_province_hourly
        .select(
            "gold_timestamp"
        )
        .distinct()
        .withColumn(
            "time_grain",
            F.lit(
                "HOUR"
            ),
        )
        .withColumn(
            "date",
            F.to_date(
                F.col(
                    "gold_timestamp"
                )
            ),
        )
        .withColumn(
            "year",
            F.year(
                F.col(
                    "gold_timestamp"
                )
            ),
        )
        .withColumn(
            "month",
            F.month(
                F.col(
                    "gold_timestamp"
                )
            ),
        )
        .withColumn(
            "year_month",
            F.date_format(
                F.col(
                    "gold_timestamp"
                ),
                "yyyy-MM",
            ),
        )
        .withColumn(
            "day",
            F.dayofmonth(
                F.col(
                    "gold_timestamp"
                )
            ),
        )
        .withColumn(
            "day_of_week",
            (
                F.pmod(
                    F.dayofweek(
                        F.col(
                            "gold_timestamp"
                        )
                    )
                    + F.lit(
                        5
                    ),
                    F.lit(
                        7
                    ),
                )
                + F.lit(
                    1
                )
            ),
        )
        .withColumn(
            "hour",
            F.hour(
                F.col(
                    "gold_timestamp"
                )
            ),
        )
        .withColumn(
            "minute",
            F.minute(
                F.col(
                    "gold_timestamp"
                )
            ),
        )
    )

    hourly = (
        add_deterministic_time_key(
            hourly,
            time_grain="HOUR",
        )
    )


    # ========================================================================
    # Month
    #
    # Monthly Gold members are represented by year_month.
    #
    # No artificial day, date, hour or timestamp is created to represent
    # a month.
    # ========================================================================

    monthly = (
        fact_installed_capacity_monthly
        .select(
            "year_month",
        )
        .distinct()
        .withColumn(
            "time_grain",
            F.lit(
                "MONTH"
            ),
        )
        .withColumn(
            "gold_timestamp",
            F.lit(
                None
            ).cast(
                "timestamp"
            ),
        )
        .withColumn(
            "date",
            F.lit(
                None
            ).cast(
                "date"
            ),
        )
        .withColumn(
            "year",
            F.substring(
                F.col(
                    "year_month"
                ),
                1,
                4,
            ).cast(
                "int"
            ),
        )
        .withColumn(
            "month",
            F.substring(
                F.col(
                    "year_month"
                ),
                6,
                2,
            ).cast(
                "int"
            ),
        )
        .withColumn(
            "day",
            F.lit(
                None
            ).cast(
                "int"
            ),
        )
        .withColumn(
            "day_of_week",
            F.lit(
                None
            ).cast(
                "int"
            ),
        )
        .withColumn(
            "hour",
            F.lit(
                None
            ).cast(
                "int"
            ),
        )
        .withColumn(
            "minute",
            F.lit(
                None
            ).cast(
                "int"
            ),
        )
    )

    monthly = (
        add_deterministic_time_key(
            monthly,
            time_grain="MONTH",
        )
    )

    # ========================================================================
    # Complete Gold time dimension
    #
    # DISTINCT above is intentional dimension projection:
    # many fact rows legitimately reference the same temporal member.
    # It is not being used to hide duplicate fact grains.
    # ========================================================================

    result = (
        hourly
        .unionByName(
            monthly
        )
        .select(
            "time_key",
            "time_grain",
            "gold_timestamp",
            "date",
            "year",
            "month",
            "year_month",
            "day",
            "day_of_week",
            "hour",
            "minute",
        )
    )

    result = (
        add_gold_created_at(
            result
        )
    )

    return result


# ============================================================================
# Gold installed-capacity monthly fact
# ============================================================================

def build_gold_fact_installed_capacity_monthly(
    spark: SparkSession,
) -> DataFrame:
    """
    Build the physical Gold CCAA x month installed-capacity fact from
    validated Silver data.

    Pipeline:

        ESIOS monthly installed capacity
            -> preserve source timestamp
            -> derive natural Gold month
            -> pivot approved installed-capacity metrics
            -> deterministic CCAA geography_key
            -> gold_created_at

    Natural grain:

        geography_key + year_month

    No automatic ESIOS +1-hour temporal correction is applied to monthly
    installed capacity.
    """

    # ========================================================================
    # Read real persisted Silver
    # ========================================================================

    source = (
        read_silver_table(
            spark=spark,
            table_name=(
                TABLE_SILVER_ESIOS_INSTALLED_CAPACITY_MONTHLY
            ),
        )
    )

    # ========================================================================
    # Gold monthly temporal structure
    # ========================================================================

    prepared_source = (
        source
        .withColumn(
            "source_timestamp",
            F.col(
                "observation_timestamp"
            ),
        )
        .withColumn(
            "gold_month_timestamp",
            F.date_trunc(
                "month",
                F.col(
                    "observation_timestamp"
                ),
            ),
        )
        .withColumn(
            "year_month",
            F.date_format(
                F.col(
                    "observation_timestamp"
                ),
                "yyyy-MM",
            ),
        )
    )

    # ========================================================================
    # Approved installed-capacity metrics
    # ========================================================================

    result = (
        prepare_installed_capacity_metrics(
            prepared_source
        )
    )

    # ========================================================================
    # Gold deterministic geography identifier
    # ========================================================================

    result = (
        add_deterministic_geography_key(
            result,
            geography_level=(
                "AUTONOMOUS_COMMUNITY"
            ),
            geography_code_column=(
                "autonomous_community_code"
            ),
        )
    )

    result = (
        add_deterministic_time_key(
            result,
            time_grain="MONTH",
        )
    )

    # ========================================================================
    # Gold technical metadata
    # ========================================================================

    result = (
        add_gold_created_at(
            result
        )
    )

    return result


# ============================================================================
# Gold geography dimension
# ============================================================================

def build_gold_dim_geography(
    spark: SparkSession,
    fact_installed_capacity_monthly: DataFrame,
) -> DataFrame:
    """
    Build the Gold geography dimension.

    Sources:

        Provinces
            -> canonical CNIG province master
            -> real ESIOS province identifier when available

        Autonomous communities
            -> canonical CNIG autonomous-community master
            -> real ESIOS identifier when available

    Natural key:

        geography_key

    No external identifier is invented. If one canonical geography is linked
    to multiple real ESIOS identifiers, the build fails explicitly.
    """

    # ========================================================================
    # Read canonical Silver geography
    # ========================================================================

    cnig_provinces = (
        read_silver_table(
            spark=spark,
            table_name=TABLE_SILVER_CNIG_PROVINCES,
        )
    )

    cnig_autonomous_communities = (
        read_silver_table(
            spark=spark,
            table_name=(
                TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES
            ),
        )
    )

    esios_energy_hourly = (
        read_silver_table(
            spark=spark,
            table_name=TABLE_SILVER_ESIOS_ENERGY_HOURLY,
        )
    )

    # ========================================================================
    # Province -> real ESIOS identifier
    # ========================================================================

    province_esios_ids = (
        esios_energy_hourly
        .select(
            "province_code",
            "esios_geo_id",
        )
        .filter(
            F.col(
                "province_code"
            ).isNotNull()
            &
            F.col(
                "esios_geo_id"
            ).isNotNull()
        )
        .distinct()
    )

    province_esios_conflicts = (
        province_esios_ids
        .groupBy(
            "province_code"
        )
        .agg(
            F.countDistinct(
                "esios_geo_id"
            ).alias(
                "esios_geo_id_count"
            )
        )
        .filter(
            F.col(
                "esios_geo_id_count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if province_esios_conflicts != 0:
        raise ValueError(
            "Cannot build Gold geography dimension: "
            f"{province_esios_conflicts} provinces map to multiple "
            "ESIOS geographical identifiers."
        )

    # ========================================================================
    # Province members
    # ========================================================================

    provinces = (
        cnig_provinces
        .select(
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        )
        .join(
            province_esios_ids,
            on="province_code",
            how="left",
        )
        .withColumn(
            "geography_level",
            F.lit(
                "PROVINCE"
            ),
        )
        .withColumn(
            "geography_code",
            F.col(
                "province_code"
            ),
        )
        .withColumn(
            "geography_name",
            F.col(
                "province_name"
            ),
        )
        .withColumn(
            "country_code",
            F.lit(
                "ES"
            ),
        )
        .withColumn(
            "country_name",
            F.lit(
                "España"
            ),
        )
    )

    provinces = (
        add_deterministic_geography_key(
            provinces,
            geography_level="PROVINCE",
            geography_code_column="province_code",
        )
    )

    # ========================================================================
    # Autonomous community -> real ESIOS identifier
    # ========================================================================

    autonomous_community_esios_ids = (
        fact_installed_capacity_monthly
        .select(
            "autonomous_community_code",
            "esios_geo_id",
        )
        .filter(
            F.col(
                "autonomous_community_code"
            ).isNotNull()
            &
            F.col(
                "esios_geo_id"
            ).isNotNull()
        )
        .distinct()
    )

    autonomous_community_esios_conflicts = (
        autonomous_community_esios_ids
        .groupBy(
            "autonomous_community_code"
        )
        .agg(
            F.countDistinct(
                "esios_geo_id"
            ).alias(
                "esios_geo_id_count"
            )
        )
        .filter(
            F.col(
                "esios_geo_id_count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if autonomous_community_esios_conflicts != 0:
        raise ValueError(
            "Cannot build Gold geography dimension: "
            f"{autonomous_community_esios_conflicts} autonomous "
            "communities map to multiple ESIOS geographical identifiers."
        )

    # ========================================================================
    # Autonomous-community members
    # ========================================================================

    autonomous_communities = (
        cnig_autonomous_communities
        .select(
            "autonomous_community_code",
            "autonomous_community_name",
        )
        .join(
            autonomous_community_esios_ids,
            on="autonomous_community_code",
            how="left",
        )
        .withColumn(
            "geography_level",
            F.lit(
                "AUTONOMOUS_COMMUNITY"
            ),
        )
        .withColumn(
            "geography_code",
            F.col(
                "autonomous_community_code"
            ),
        )
        .withColumn(
            "geography_name",
            F.col(
                "autonomous_community_name"
            ),
        )
        .withColumn(
            "province_code",
            F.lit(
                None
            ).cast(
                "string"
            ),
        )
        .withColumn(
            "province_name",
            F.lit(
                None
            ).cast(
                "string"
            ),
        )
        .withColumn(
            "country_code",
            F.lit(
                "ES"
            ),
        )
        .withColumn(
            "country_name",
            F.lit(
                "España"
            ),
        )
    )

    autonomous_communities = (
        add_deterministic_geography_key(
            autonomous_communities,
            geography_level="AUTONOMOUS_COMMUNITY",
            geography_code_column=(
                "autonomous_community_code"
            ),
        )
    )


    # ========================================================================
    # Complete Gold geography dimension
    # ========================================================================

    geography_columns = [
        "geography_key",
        "geography_level",
        "geography_code",
        "geography_name",
        "province_code",
        "province_name",
        "autonomous_community_code",
        "autonomous_community_name",
        "country_code",
        "country_name",
        "esios_geo_id",
    ]

    result = (
        provinces
        .select(
            *geography_columns
        )
        .unionByName(
            autonomous_communities
            .select(
                *geography_columns
            )
        )
    )

    # ========================================================================
    # Final dimension-key validation
    # ========================================================================

    null_key_count = (
        result
        .filter(
            F.col(
                "geography_key"
            ).isNull()
        )
        .count()
    )

    if null_key_count != 0:
        raise ValueError(
            "Cannot build Gold geography dimension: "
            f"{null_key_count} rows have NULL geography_key."
        )

    duplicate_key_count = (
        result
        .groupBy(
            "geography_key"
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if duplicate_key_count != 0:
        raise ValueError(
            "Cannot build Gold geography dimension: "
            f"{duplicate_key_count} duplicated geography keys found."
        )

    result = (
        add_gold_created_at(
            result
        )
    )

    return result


# ============================================================================
# Gold dimension business-key validation
# ============================================================================

def validate_gold_dim_time_business_keys(
    df: DataFrame,
) -> None:
    """
    Validate the approved natural business grains of gold_dim_time.

    Submonthly grains:

        time_grain + gold_timestamp

    Monthly grain:

        time_grain + year_month
    """

    # ------------------------------------------------------------------------
    # Submonthly temporal members
    # ------------------------------------------------------------------------

    submonthly = (
        df
        .filter(
            F.col(
                "time_grain"
            )
            != F.lit(
                "MONTH"
            )
        )
    )

    null_submonthly_keys = (
        submonthly
        .filter(
            F.col(
                "time_grain"
            ).isNull()
            |
            F.col(
                "gold_timestamp"
            ).isNull()
        )
        .count()
    )

    if null_submonthly_keys != 0:
        raise ValueError(
            "Gold time dimension contains "
            f"{null_submonthly_keys} submonthly rows "
            "with incomplete business keys."
        )

    duplicate_submonthly_keys = (
        submonthly
        .groupBy(
            "time_grain",
            "gold_timestamp",
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if duplicate_submonthly_keys != 0:
        raise ValueError(
            "Gold time dimension contains "
            f"{duplicate_submonthly_keys} duplicated "
            "submonthly business keys."
        )

    # ------------------------------------------------------------------------
    # Monthly temporal members
    # ------------------------------------------------------------------------

    monthly = (
        df
        .filter(
            F.col(
                "time_grain"
            )
            == F.lit(
                "MONTH"
            )
        )
    )

    null_monthly_keys = (
        monthly
        .filter(
            F.col(
                "year_month"
            ).isNull()
        )
        .count()
    )

    if null_monthly_keys != 0:
        raise ValueError(
            "Gold time dimension contains "
            f"{null_monthly_keys} monthly rows "
            "without year_month."
        )

    duplicate_monthly_keys = (
        monthly
        .groupBy(
            "time_grain",
            "year_month",
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if duplicate_monthly_keys != 0:
        raise ValueError(
            "Gold time dimension contains "
            f"{duplicate_monthly_keys} duplicated "
            "monthly business keys."
        )


def validate_gold_dim_geography_business_keys(
    df: DataFrame,
) -> None:
    """
    Validate the approved natural business grain of gold_dim_geography:

        geography_level + geography_code
    """

    null_business_keys = (
        df
        .filter(
            F.col(
                "geography_level"
            ).isNull()
            |
            F.col(
                "geography_code"
            ).isNull()
        )
        .count()
    )

    if null_business_keys != 0:
        raise ValueError(
            "Gold geography dimension contains "
            f"{null_business_keys} rows with incomplete "
            "business geography keys."
        )

    duplicate_business_keys = (
        df
        .groupBy(
            "geography_level",
            "geography_code",
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if duplicate_business_keys != 0:
        raise ValueError(
            "Gold geography dimension contains "
            f"{duplicate_business_keys} duplicated "
            "business geography keys."
        )


# ============================================================================
# Source-key validation
# ============================================================================

def validate_source_keys(
    df: DataFrame,
    natural_key: list[str],
    table_name: str,
) -> None:
    """
    Reject a Gold source DataFrame whose natural key is incomplete.
    """
    missing_columns = [
        column
        for column
        in natural_key
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing natural-key columns for {table_name}: "
            f"{missing_columns}"
        )

    null_condition = " OR ".join(
        f"`{column}` IS NULL"
        for column
        in natural_key
    )

    null_key_count = (
        df
        .filter(
            null_condition
        )
        .count()
    )

    if null_key_count != 0:
        raise ValueError(
            f"Cannot write {table_name}: "
            f"{null_key_count} rows contain NULL natural keys."
        )


# ============================================================================
# Duplicate validation
# ============================================================================

def validate_source_duplicates(
    df: DataFrame,
    natural_key: list[str],
    table_name: str,
) -> None:
    """
    Reject duplicate analytical natural keys before Gold persistence.

    Gold must never use deduplication to hide an invalid analytical grain.
    """
    duplicate_count = (
        df
        .groupBy(
            *natural_key
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(
            f"Cannot write {table_name}: "
            f"{duplicate_count} duplicated natural keys found."
        )


# ============================================================================
# Target-schema validation
# ============================================================================

def validate_source_schema(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
) -> list[str]:
    """
    Validate that the Gold source DataFrame exactly matches the physical
    target-table column set.

    Column ordering may differ because the MERGE statement references every
    target column explicitly by name.
    """
    validate_table_exists(
        spark=spark,
        table_name=table_name,
    )

    target_columns = (
        spark
        .table(
            table_name
        )
        .columns
    )

    source_columns = (
        df.columns
    )

    missing_columns = [
        column
        for column
        in target_columns
        if column not in source_columns
    ]

    unexpected_columns = [
        column
        for column
        in source_columns
        if column not in target_columns
    ]

    if missing_columns:
        raise ValueError(
            f"Cannot write {table_name}: "
            f"missing target columns {missing_columns}."
        )

    if unexpected_columns:
        raise ValueError(
            f"Cannot write {table_name}: "
            f"unexpected source columns {unexpected_columns}."
        )

    return target_columns


# ============================================================================
# Persisted-grain validation
# ============================================================================

def validate_persisted_natural_key(
    spark: SparkSession,
    table_name: str,
    natural_key: list[str],
) -> None:
    """
    Validate that the persisted Gold Iceberg table still contains exactly
    one row per approved natural key.
    """
    target = (
        spark
        .table(
            table_name
        )
    )

    duplicate_count = (
        target
        .groupBy(
            *natural_key
        )
        .count()
        .filter(
            F.col(
                "count"
            )
            > F.lit(
                1
            )
        )
        .count()
    )

    if duplicate_count != 0:
        raise RuntimeError(
            f"Persisted Gold table {table_name} contains "
            f"{duplicate_count} duplicated natural keys."
        )


# ============================================================================
# Gold Iceberg MERGE
# ============================================================================

def merge_into_gold_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    view_name: str,
) -> None:
    """
    Persist one validated Gold DataFrame through an idempotent Iceberg MERGE.

    Approved behavior:

        MATCH
            -> update analytical/structural columns
            -> preserve original gold_created_at

        NOT MATCH
            -> insert the complete new Gold row

    Blind append is prohibited.
    """
    if table_name not in GOLD_NATURAL_KEYS:
        raise ValueError(
            "No approved Gold natural key registered for table: "
            f"{table_name}"
        )

    natural_key = (
        GOLD_NATURAL_KEYS[
            table_name
        ]
    )

    validate_table_exists(
        spark=spark,
        table_name=table_name,
    )

    validate_source_keys(
        df,
        natural_key,
        table_name,
    )

    validate_source_duplicates(
        df,
        natural_key,
        table_name,
    )

    target_columns = (
        validate_source_schema(
            spark,
            df,
            table_name,
        )
    )

    # ------------------------------------------------------------------------
    # Materialize the complete Gold source before MERGE.
    #
    # This also fixes current_timestamp() for the duration of this source
    # execution instead of leaving it as an unresolved expression inside
    # the MERGE plan.
    # ------------------------------------------------------------------------

    materialized_df = (
        df
        .select(
            *target_columns
        )
        .localCheckpoint(
            eager=True
        )
    )

    source_count = (
        materialized_df
        .count()
    )

    print(
        "=" * 80
    )

    print(
        f"TABLE = {table_name}"
    )

    print(
        f"SOURCE_ROWS = {source_count}"
    )

    materialized_df.createOrReplaceTempView(
        view_name
    )

    # ------------------------------------------------------------------------
    # Natural-key match
    # ------------------------------------------------------------------------

    merge_condition = " AND ".join(
        (
            f"target.`{column}` "
            f"= source.`{column}`"
        )
        for column
        in natural_key
    )

    # ------------------------------------------------------------------------
    # MATCHED rows
    #
    # gold_created_at is deliberately excluded.
    # Its value represents creation of the Gold member, not reprocessing.
    # ------------------------------------------------------------------------

    update_columns = [
        column
        for column
        in target_columns
        if column
        != GOLD_CREATED_AT_COLUMN
    ]

    update_assignments = ",\n                ".join(
        (
            f"target.`{column}` "
            f"= source.`{column}`"
        )
        for column
        in update_columns
    )

    # ------------------------------------------------------------------------
    # New rows
    # ------------------------------------------------------------------------

    insert_columns = ", ".join(
        f"`{column}`"
        for column
        in target_columns
    )

    insert_values = ", ".join(
        f"source.`{column}`"
        for column
        in target_columns
    )

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING {view_name} AS source
        ON {merge_condition}

        WHEN MATCHED THEN
            UPDATE SET
                {update_assignments}

        WHEN NOT MATCHED THEN
            INSERT (
                {insert_columns}
            )
            VALUES (
                {insert_values}
            )
        """
    )

    target_count = (
        spark
        .table(
            table_name
        )
        .count()
    )

    validate_persisted_natural_key(
        spark,
        table_name,
        natural_key,
    )

    print(
        f"TARGET_ROWS_AFTER_MERGE = {target_count}"
    )

    print(
        f"MERGED = {table_name}"
    )


# ============================================================================
# Main Gold persistence
# ============================================================================

def main() -> None:
    """
    Build, validate and persist the complete physical Gold model.

    Execution order:

        1. Build the two Gold facts from real persisted Silver.
        2. Materialize the facts.
        3. Build the two dimensions from the real Gold datasets.
        4. Validate every dataset before the first physical write.
        5. Persist all four tables through idempotent Iceberg MERGE.
        6. Validate persisted row counts and natural-key uniqueness.

    No blind append is performed.
    """

    spark = (
        SparkSession.builder
        .appName(
            "gold-persistence"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    datasets = []

    try:
        print(
            "=" * 80
        )

        print(
            "BUILD GOLD PHYSICAL DATASETS"
        )

        print(
            "=" * 80
        )

        # ====================================================================
        # Build Gold facts
        # ====================================================================

        fact_province_hourly = (
            build_gold_fact_province_hourly(
                spark
            )
            .cache()
        )

        fact_installed_capacity_monthly = (
            build_gold_fact_installed_capacity_monthly(
                spark
            )
            .cache()
        )

        # ====================================================================
        # Materialize facts
        #
        # This detects transformation/runtime errors before any physical
        # Gold table is written.
        # ====================================================================

        fact_province_hourly_count = (
            fact_province_hourly.count()
        )

        fact_installed_capacity_monthly_count = (
            fact_installed_capacity_monthly.count()
        )

        print(
            "FACT_PROVINCE_HOURLY_ROWS = "
            f"{fact_province_hourly_count}"
        )

        print(
            "FACT_INSTALLED_CAPACITY_MONTHLY_ROWS = "
            f"{fact_installed_capacity_monthly_count}"
        )

        # ====================================================================
        # Build Gold dimensions from materialized facts
        # ====================================================================

        dim_time = (
            build_gold_dim_time(
                fact_province_hourly,
                fact_installed_capacity_monthly,
            )
            .cache()
        )

        dim_geography = (
            build_gold_dim_geography(
                spark,
                fact_installed_capacity_monthly,
            )
            .cache()
        )

        dim_time_count = (
            dim_time.count()
        )

        dim_geography_count = (
            dim_geography.count()
        )

        # ====================================================================
        # Dimension business-key validation
        # ====================================================================

        validate_gold_dim_time_business_keys(
            dim_time
        )

        validate_gold_dim_geography_business_keys(
            dim_geography
        )

        print(
            "DIMENSION BUSINESS KEYS VALIDATED"
        )

        print(
            "DIM_TIME_ROWS = "
            f"{dim_time_count}"
        )

        print(
            "DIM_GEOGRAPHY_ROWS = "
            f"{dim_geography_count}"
        )

        # ====================================================================
        # Complete physical dataset registry
        # ====================================================================

        datasets = [
            (
                TABLE_GOLD_FACT_PROVINCE_HOURLY,
                fact_province_hourly,
                "gold_fact_province_hourly_source",
            ),
            (
                TABLE_GOLD_FACT_INSTALLED_CAPACITY_MONTHLY,
                fact_installed_capacity_monthly,
                "gold_fact_installed_capacity_monthly_source",
            ),
            (
                TABLE_GOLD_DIM_TIME,
                dim_time,
                "gold_dim_time_source",
            ),
            (
                TABLE_GOLD_DIM_GEOGRAPHY,
                dim_geography,
                "gold_dim_geography_source",
            ),
        ]

        # ====================================================================
        # Pre-write validation
        #
        # ALL FOUR datasets are validated before the first MERGE.
        # ====================================================================

        print(
            "=" * 80
        )

        print(
            "PRE-WRITE GOLD VALIDATION"
        )

        print(
            "=" * 80
        )

        for (
            table_name,
            dataframe,
            _,
        ) in datasets:
            natural_key = (
                GOLD_NATURAL_KEYS[
                    table_name
                ]
            )

            validate_source_keys(
                dataframe,
                natural_key,
                table_name,
            )

            validate_source_duplicates(
                dataframe,
                natural_key,
                table_name,
            )

            validate_source_schema(
                spark,
                dataframe,
                table_name,
            )

            print(
                f"VALIDATED = {table_name}"
            )

        print(
            "ALL GOLD DATASETS PASSED PRE-WRITE VALIDATION"
        )

        # ====================================================================
        # Physical Iceberg persistence
        # ====================================================================

        print(
            "=" * 80
        )

        print(
            "PERSIST GOLD TO ICEBERG / MINIO"
        )

        print(
            "=" * 80
        )

        for (
            table_name,
            dataframe,
            view_name,
        ) in datasets:
            merge_into_gold_table(
                spark=spark,
                df=dataframe,
                table_name=table_name,
                view_name=view_name,
            )

        # ====================================================================
        # Final persisted counts
        # ====================================================================

        print(
            "=" * 80
        )

        print(
            "FINAL PERSISTED GOLD COUNTS"
        )

        print(
            "=" * 80
        )

        for (
            table_name,
            _,
            _,
        ) in datasets:
            persisted_count = (
                spark
                .table(
                    table_name
                )
                .count()
            )

            print(
                f"{table_name} = {persisted_count}"
            )

        print(
            "=" * 80
        )

        print(
            "GOLD PERSISTENCE COMPLETED"
        )

        print(
            "=" * 80
        )

    finally:
        for dataset in datasets:
            dataframe = dataset[
                1
            ]

            dataframe.unpersist()

        spark.stop()


# ============================================================================
# Entry point
# ============================================================================

if __name__ == "__main__":
    main()