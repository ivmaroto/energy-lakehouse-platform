from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

import gold.write_gold as write_gold

from gold.geography import (
    COUNTRY_ES_GEOGRAPHY_KEY,
    PENINSULA_ES_GEOGRAPHY_KEY,
)

from gold.metrics import (
    INSTALLED_CAPACITY_METRICS,
)


# ============================================================================
# Spark
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName(
            "gold-write-tests"
        )
        .config(
            "spark.pyspark.python",
            python_executable,
        )
        .config(
            "spark.pyspark.driver.python",
            python_executable,
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel(
        "ERROR"
    )

    yield session

    session.stop()


# ============================================================================
# Gold technical metadata
# ============================================================================

def test_add_gold_created_at_adds_non_null_timestamp(
    spark,
):
    df = spark.createDataFrame(
        [
            ("20",),
        ],
        [
            "province_code",
        ],
    )

    result = (
        write_gold.add_gold_created_at(
            df
        )
        .first()
    )

    assert (
        result["gold_created_at"]
        is not None
    )


# ============================================================================
# Natural-key validation
# ============================================================================

def test_validate_source_keys_rejects_missing_key_column(
    spark,
):
    df = spark.createDataFrame(
        [
            ("20",),
        ],
        [
            "province_code",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Missing natural-key columns",
    ):
        write_gold.validate_source_keys(
            df,
            [
                "province_code",
                "gold_timestamp",
            ],
            "test_table",
        )


def test_validate_source_keys_rejects_null_key(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                None,
                datetime(
                    2026,
                    8,
                    24,
                    10,
                    0,
                ),
            ),
        ],
        "province_code string, "
        "gold_timestamp timestamp",
    )

    with pytest.raises(
        ValueError,
        match="NULL natural keys",
    ):
        write_gold.validate_source_keys(
            df,
            [
                "province_code",
                "gold_timestamp",
            ],
            "test_table",
        )


def test_validate_source_duplicates_rejects_duplicate_grain(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    df = spark.createDataFrame(
        [
            (
                "20",
                timestamp,
            ),
            (
                "20",
                timestamp,
            ),
        ],
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicated natural keys",
    ):
        write_gold.validate_source_duplicates(
            df,
            [
                "province_code",
                "gold_timestamp",
            ],
            "test_table",
        )


def test_validate_persisted_natural_key_rejects_duplicates(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    target = spark.createDataFrame(
        [
            (
                "20",
                timestamp,
            ),
            (
                "20",
                timestamp,
            ),
        ],
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    class FakeSpark:
        def table(
            self,
            table_name,
        ):
            return target

    with pytest.raises(
        RuntimeError,
        match="duplicated natural keys",
    ):
        write_gold.validate_persisted_natural_key(
            FakeSpark(),
            "test_table",
            [
                "province_code",
                "gold_timestamp",
            ],
        )


# ============================================================================
# Gold time dimension
# ============================================================================

def test_build_gold_dim_time_builds_all_approved_grains(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    fact_province_hourly = spark.createDataFrame(
        [(timestamp,)],
        ["gold_timestamp"],
    )

    fact_installed_capacity_monthly = spark.createDataFrame(
        [("2026-08",)],
        ["year_month"],
    )

    result = (
        write_gold.build_gold_dim_time(
            fact_province_hourly,
            fact_installed_capacity_monthly,
        )
        .cache()
    )

    try:
        assert result.count() == 2

        assert (
            result
            .select("time_key")
            .distinct()
            .count()
            == 2
        )

        grains = {
            row["time_grain"]
            for row in result
            .select("time_grain")
            .collect()
        }

        assert grains == {
            "HOUR",
            "MONTH",
        }

        hourly = (
            result
            .filter("time_grain = 'HOUR'")
            .first()
        )

        assert hourly["gold_timestamp"] == timestamp
        assert hourly["day_of_week"] == 1
        assert hourly["year"] == 2026
        assert hourly["month"] == 8
        assert hourly["day"] == 24
        assert hourly["hour"] == 10
        assert hourly["minute"] == 0
        assert len(hourly["time_key"]) == 64

        monthly = (
            result
            .filter("time_grain = 'MONTH'")
            .first()
        )

        assert monthly["year_month"] == "2026-08"
        assert monthly["year"] == 2026
        assert monthly["month"] == 8
        assert monthly["gold_timestamp"] is None
        assert monthly["date"] is None
        assert monthly["day"] is None
        assert monthly["day_of_week"] is None
        assert monthly["hour"] is None
        assert monthly["minute"] is None
        assert len(monthly["time_key"]) == 64

    finally:
        result.unpersist()



def test_validate_gold_dim_time_rejects_duplicate_submonthly_business_key(
    spark,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    df = spark.createDataFrame(
        [
            (
                "HOUR",
                timestamp,
                "2026-08",
            ),
            (
                "HOUR",
                timestamp,
                "2026-08",
            ),
        ],
        "time_grain string, "
        "gold_timestamp timestamp, "
        "year_month string",
    )

    with pytest.raises(
        ValueError,
        match="duplicated submonthly business keys",
    ):
        write_gold.validate_gold_dim_time_business_keys(
            df
        )


def test_validate_gold_dim_time_rejects_duplicate_month_business_key(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "MONTH",
                None,
                "2026-08",
            ),
            (
                "MONTH",
                None,
                "2026-08",
            ),
        ],
        "time_grain string, "
        "gold_timestamp timestamp, "
        "year_month string",
    )

    with pytest.raises(
        ValueError,
        match="duplicated monthly business keys",
    ):
        write_gold.validate_gold_dim_time_business_keys(
            df
        )


# ============================================================================
# Gold geography business keys
# ============================================================================

def test_validate_gold_dim_geography_rejects_duplicate_business_key(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "PROVINCE",
                "20",
            ),
            (
                "PROVINCE",
                "20",
            ),
        ],
        [
            "geography_level",
            "geography_code",
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicated business geography keys",
    ):
        write_gold.validate_gold_dim_geography_business_keys(
            df
        )


def test_validate_gold_dim_geography_rejects_null_business_key(
    spark,
):
    df = spark.createDataFrame(
        [
            (
                "PROVINCE",
                None,
            ),
        ],
        "geography_level string, "
        "geography_code string",
    )

    with pytest.raises(
        ValueError,
        match="incomplete business geography keys",
    ):
        write_gold.validate_gold_dim_geography_business_keys(
            df
        )


# ============================================================================
# Installed-capacity monthly fact
# ============================================================================

def test_build_gold_fact_installed_capacity_monthly(
    spark,
    monkeypatch,
):
    timestamp = datetime(
        2026,
        8,
        1,
        0,
        0,
    )

    source_rows = []

    for (
        indicator_id,
        _,
    ) in INSTALLED_CAPACITY_METRICS.items():
        source_rows.append(
            (
                timestamp,
                indicator_id,
                1600,
                "16",
                "PaÃ­s Vasco/Euskadi",
                1000.0,
            )
        )

    source = spark.createDataFrame(
        source_rows,
        [
            "observation_timestamp",
            "indicator_id",
            "esios_geo_id",
            "autonomous_community_code",
            "autonomous_community_name",
            "value",
        ],
    )

    monkeypatch.setattr(
        write_gold,
        "read_silver_table",
        lambda **kwargs: source,
    )

    result = (
        write_gold
        .build_gold_fact_installed_capacity_monthly(
            spark
        )
        .cache()
    )

    try:
        assert result.count() == 1

        row = result.first()

        assert (
            row["year_month"]
            == "2026-08"
        )

        assert (
            row["gold_month_timestamp"]
            == timestamp
        )

        assert (
            row["source_timestamp"]
            == timestamp
        )

        assert (
            row["autonomous_community_code"]
            == "16"
        )

        assert len(
            row["geography_key"]
        ) == 64

        assert (
            row["gold_created_at"]
            is not None
        )

    finally:
        result.unpersist()


# ============================================================================
# Country 5-minute fact
# ============================================================================



# ============================================================================
# Province-hourly builder orchestration
# ============================================================================

def test_build_gold_fact_province_hourly_adds_key_and_uses_gap(
    spark,
    monkeypatch,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    dummy = spark.createDataFrame(
        [
            (
                1,
            ),
        ],
        [
            "dummy",
        ],
    )

    weather = spark.createDataFrame(
        [
            (
                "20",
                timestamp,
            ),
        ],
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    energy = spark.createDataFrame(
        [
            (
                "20",
                timestamp,
            ),
        ],
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    integrated = spark.createDataFrame(
        [
            (
                "20",
                timestamp,
            ),
        ],
        [
            "province_code",
            "gold_timestamp",
        ],
    )

    read_tables = []
    used_gap = []

    def fake_read_silver_table(
        *,
        spark,
        table_name,
    ):
        read_tables.append(
            table_name
        )

        return dummy

    def fake_apply_esios_time_gap(
        df,
        gap_hours,
        **kwargs,
    ):
        used_gap.append(
            gap_hours
        )

        return df

    monkeypatch.setattr(
        write_gold,
        "read_silver_table",
        fake_read_silver_table,
    )

    monkeypatch.setattr(
        write_gold,
        "get_esios_time_gap_hours",
        lambda: 1,
    )

    monkeypatch.setattr(
        write_gold,
        "prepare_province_hourly_weather",
        lambda *args: weather,
    )

    monkeypatch.setattr(
        write_gold,
        "apply_esios_time_gap",
        fake_apply_esios_time_gap,
    )

    monkeypatch.setattr(
        write_gold,
        "prepare_hourly_energy_metrics",
        lambda df: energy,
    )

    monkeypatch.setattr(
        write_gold,
        "integrate_province_hourly_weather_energy",
        lambda weather_df, energy_df: integrated,
    )

    result = (
        write_gold
        .build_gold_fact_province_hourly(
            spark
        )
        .first()
    )

    assert used_gap == [
        1
    ]

    assert set(
        read_tables
    ) == {
        write_gold.TABLE_SILVER_AEMET_CURRENT,
        write_gold.TABLE_SILVER_AEMET_STATIONS,
        write_gold.TABLE_SILVER_OPEN_METEO_HOURLY,
        write_gold.TABLE_SILVER_OPEN_METEO_15MIN,
        write_gold.TABLE_SILVER_ESIOS_ENERGY_HOURLY,
    }

    assert (
        result["province_code"]
        == "20"
    )

    assert len(
        result["geography_key"]
    ) == 64

    assert (
        result["gold_created_at"]
        is not None
    )


# ============================================================================
# Country 15-minute builder orchestration
# ============================================================================



# ============================================================================
# Geography-dimension synthetic inputs
# ============================================================================

def build_synthetic_geography_inputs(
    spark,
    *,
    conflicting_province_esios_id: bool = False,
):
    cnig_provinces = spark.createDataFrame(
        [
            (
                "20",
                "Gipuzkoa",
                "16",
                "Pa?s Vasco/Euskadi",
            ),
            (
                "28",
                "Madrid",
                "13",
                "Comunidad de Madrid",
            ),
        ],
        [
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    cnig_autonomous_communities = spark.createDataFrame(
        [
            (
                "16",
                "Pa?s Vasco/Euskadi",
            ),
            (
                "13",
                "Comunidad de Madrid",
            ),
        ],
        [
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )

    esios_rows = [
        ("20", 2000),
        ("28", 2800),
    ]

    if conflicting_province_esios_id:
        esios_rows.append(
            ("20", 9999)
        )

    esios_energy_hourly = spark.createDataFrame(
        esios_rows,
        [
            "province_code",
            "esios_geo_id",
        ],
    )

    fact_installed_capacity_monthly = spark.createDataFrame(
        [
            ("16", 1600),
            ("13", 1300),
        ],
        [
            "autonomous_community_code",
            "esios_geo_id",
        ],
    )

    return (
        cnig_provinces,
        cnig_autonomous_communities,
        esios_energy_hourly,
        fact_installed_capacity_monthly,
    )



# ============================================================================
# Gold geography dimension
# ============================================================================

def test_build_gold_dim_geography_builds_all_levels(
    spark,
    monkeypatch,
):
    (
        cnig_provinces,
        cnig_autonomous_communities,
        esios_energy_hourly,
        fact_installed_capacity_monthly,
    ) = build_synthetic_geography_inputs(
        spark
    )

    source_tables = {
        write_gold.TABLE_SILVER_CNIG_PROVINCES: (
            cnig_provinces
        ),
        write_gold.TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES: (
            cnig_autonomous_communities
        ),
        write_gold.TABLE_SILVER_ESIOS_ENERGY_HOURLY: (
            esios_energy_hourly
        ),
    }

    monkeypatch.setattr(
        write_gold,
        "read_silver_table",
        lambda spark, table_name: (
            source_tables[table_name]
        ),
    )

    result = (
        write_gold.build_gold_dim_geography(
            spark,
            fact_installed_capacity_monthly,
        )
        .cache()
    )

    try:
        assert result.count() == 4

        level_counts = {
            row["geography_level"]: row["count"]
            for row in result
            .groupBy("geography_level")
            .count()
            .collect()
        }

        assert level_counts == {
            "PROVINCE": 2,
            "AUTONOMOUS_COMMUNITY": 2,
        }

        assert (
            result
            .select("geography_key")
            .distinct()
            .count()
            == 4
        )

        province = (
            result
            .filter(
                "geography_level = 'PROVINCE' "
                "AND province_code = '20'"
            )
            .first()
        )

        assert province["province_name"] == "Gipuzkoa"
        assert len(province["geography_key"]) == 64

        autonomous_community = (
            result
            .filter(
                "geography_level = 'AUTONOMOUS_COMMUNITY' "
                "AND autonomous_community_code = '16'"
            )
            .first()
        )

        assert (
            autonomous_community[
                "autonomous_community_name"
            ]
            == "Pa?s Vasco/Euskadi"
        )

        assert len(
            autonomous_community["geography_key"]
        ) == 64

        assert (
            result
            .filter(
                "geography_level IN ('COUNTRY', 'PENINSULA')"
            )
            .count()
            == 0
        )

        assert (
            result
            .filter("gold_created_at IS NULL")
            .count()
            == 0
        )

    finally:
        result.unpersist()



def test_build_gold_dim_geography_rejects_multiple_esios_ids_per_province(
    spark,
    monkeypatch,
):
    (
        cnig_provinces,
        cnig_autonomous_communities,
        esios_energy_hourly,
        fact_installed_capacity_monthly,
    ) = build_synthetic_geography_inputs(
        spark,
        conflicting_province_esios_id=True,
    )

    source_tables = {
        write_gold.TABLE_SILVER_CNIG_PROVINCES: (
            cnig_provinces
        ),
        write_gold.TABLE_SILVER_CNIG_AUTONOMOUS_COMMUNITIES: (
            cnig_autonomous_communities
        ),
        write_gold.TABLE_SILVER_ESIOS_ENERGY_HOURLY: (
            esios_energy_hourly
        ),
    }

    monkeypatch.setattr(
        write_gold,
        "read_silver_table",
        lambda spark, table_name: (
            source_tables[table_name]
        ),
    )

    with pytest.raises(
        ValueError,
        match="provinces map to multiple ESIOS",
    ):
        write_gold.build_gold_dim_geography(
            spark,
            fact_installed_capacity_monthly,
        )



# ============================================================================
# Gold MERGE SQL
# ============================================================================

def test_merge_into_gold_table_preserves_original_gold_created_at(
    spark,
    monkeypatch,
):
    timestamp = datetime(
        2026,
        8,
        24,
        10,
        0,
    )

    created_at = datetime(
        2026,
        8,
        25,
        20,
        0,
    )

    geography_key = "a" * 64

    source = spark.createDataFrame(
        [
            (
                geography_key,
                timestamp,
                created_at,
            ),
        ],
        [
            "geography_key",
            "gold_timestamp",
            "gold_created_at",
        ],
    )

    class FakeSpark:
        def __init__(
            self,
            target,
        ):
            self.target = target
            self.queries = []

        def sql(
            self,
            query,
        ):
            self.queries.append(
                query
            )

        def table(
            self,
            table_name,
        ):
            return self.target

    fake_spark = FakeSpark(
        source
    )

    monkeypatch.setattr(
        write_gold,
        "validate_table_exists",
        lambda **kwargs: None,
    )

    monkeypatch.setattr(
        write_gold,
        "validate_source_schema",
        lambda spark, df, table_name: (
            df.columns
        ),
    )

    monkeypatch.setattr(
        write_gold,
        "validate_persisted_natural_key",
        lambda spark, table_name, natural_key: None,
    )

    write_gold.merge_into_gold_table(
        spark=fake_spark,
        df=source,
        table_name=(
            write_gold.TABLE_GOLD_FACT_PROVINCE_HOURLY
        ),
        view_name="test_gold_merge_source",
    )

    assert len(fake_spark.queries) == 1

    merge_sql = fake_spark.queries[0]

    matched_section = (
        merge_sql
        .split("WHEN NOT MATCHED")[0]
    )

    insert_section = (
        merge_sql
        .split("WHEN NOT MATCHED")[1]
    )

    assert (
        "`gold_created_at`"
        not in matched_section
    )

    assert (
        "`gold_created_at`"
        in insert_section
    )

    assert (
        "target.`geography_key` "
        "= source.`geography_key`"
        in merge_sql
    )

    assert (
        "target.`gold_timestamp` "
        "= source.`gold_timestamp`"
        in merge_sql
    )
