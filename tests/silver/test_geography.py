import json
import os
import sys

import pytest
from pyspark.sql import SparkSession

from silver.geography import (
    PROVINCE_ALIASES_PATH,
    enrich_with_cnig_province,
    load_province_aliases,
    normalize_geographical_name,
    validate_all_provinces_matched,
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
        .appName("silver-geography-tests")
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

    yield session

    session.stop()


# ============================================================================
# Synthetic CNIG master
# ============================================================================

@pytest.fixture(scope="session")
def cnig_provinces(spark):
    """
    Synthetic CNIG province master containing:
        - deterministic-normalization matches;
        - the five validated controlled-alias targets;
        - canonical province and autonomous-community identifiers.
    """
    return spark.createDataFrame(
        [
            (
                "01",
                "Araba/Álava",
                "16",
                "País Vasco/Euskadi",
            ),
            (
                "03",
                "Alacant/Alicante",
                "10",
                "Comunitat Valenciana",
            ),
            (
                "04",
                "Almería",
                "01",
                "Andalucía",
            ),
            (
                "05",
                "Ávila",
                "07",
                "Castilla y León",
            ),
            (
                "07",
                "Illes Balears",
                "04",
                "Illes Balears",
            ),
            (
                "10",
                "Cáceres",
                "11",
                "Extremadura",
            ),
            (
                "11",
                "Cádiz",
                "01",
                "Andalucía",
            ),
            (
                "12",
                "Castelló/Castellón",
                "10",
                "Comunitat Valenciana",
            ),
            (
                "14",
                "Córdoba",
                "01",
                "Andalucía",
            ),
            (
                "23",
                "Jaén",
                "01",
                "Andalucía",
            ),
            (
                "24",
                "León",
                "07",
                "Castilla y León",
            ),
            (
                "28",
                "Madrid",
                "13",
                "Comunidad de Madrid",
            ),
            (
                "29",
                "Málaga",
                "01",
                "Andalucía",
            ),
            (
                "38",
                "Santa Cruz de Tenerife",
                "05",
                "Canarias",
            ),
            (
                "46",
                "València/Valencia",
                "10",
                "Comunitat Valenciana",
            ),
        ],
        [
            "province_code",
            "province_name",
            "autonomous_community_code",
            "autonomous_community_name",
        ],
    )


# ============================================================================
# Shared Spark results
#
# These fixtures execute each logical Spark scenario only once.
# Individual parametrized tests still validate every original case.
# ============================================================================

@pytest.fixture(scope="session")
def direct_match_rows(
    spark,
    cnig_provinces,
):
    source_df = spark.createDataFrame(
        [
            ("direct-01", "CORDOBA"),
            ("direct-02", "ALMERIA"),
            ("direct-03", "ARABA/ALAVA"),
            ("direct-04", "AVILA"),
            ("direct-05", "CACERES"),
            ("direct-06", "CADIZ"),
            ("direct-07", "JAEN"),
            ("direct-08", "LEON"),
            ("direct-09", "MALAGA"),
            ("direct-10", "MADRID"),
        ],
        [
            "test_id",
            "provincia",
        ],
    )

    result = enrich_with_cnig_province(
        source_df,
        cnig_provinces,
        source_province_column="provincia",
    )

    return {
        row["provincia"]: row
        for row in result.collect()
    }


@pytest.fixture(scope="session")
def alias_match_rows(
    spark,
    cnig_provinces,
):
    source_df = spark.createDataFrame(
        [
            ("alias-01", "ALICANTE"),
            ("alias-02", "BALEARES"),
            ("alias-03", "CASTELLON"),
            ("alias-04", "STA. CRUZ DE TENERIFE"),
            ("alias-05", "VALENCIA"),
        ],
        [
            "test_id",
            "provincia",
        ],
    )

    result = enrich_with_cnig_province(
        source_df,
        cnig_provinces,
        source_province_column="provincia",
    )

    return {
        row["provincia"]: row
        for row in result.collect()
    }


@pytest.fixture(scope="session")
def open_meteo_style_row(
    spark,
    cnig_provinces,
):
    """
    Validate that the same resolver also works when the source
    province column is named 'province', as in Open-Meteo.
    """
    source_df = spark.createDataFrame(
        [
            (
                "station-1",
                "VALENCIA",
            ),
        ],
        [
            "station_id",
            "province",
        ],
    )

    result = enrich_with_cnig_province(
        source_df,
        cnig_provinces,
        source_province_column="province",
    )

    return result.first()


@pytest.fixture(scope="session")
def unknown_province_result(
    spark,
    cnig_provinces,
):
    source_df = spark.createDataFrame(
        [
            (
                "station-unknown",
                "PROVINCIA_INEXISTENTE",
            ),
        ],
        [
            "station_id",
            "provincia",
        ],
    )

    return enrich_with_cnig_province(
        source_df,
        cnig_provinces,
        source_province_column="provincia",
    )


# ============================================================================
# Deterministic normalization
# ============================================================================

@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("CORDOBA", "CORDOBA"),
        ("Córdoba", "CORDOBA"),
        (" ALMERIA ", "ALMERIA"),
        ("Almería", "ALMERIA"),
        ("ARABA/ALAVA", "ARABA/ALAVA"),
        ("Araba/Álava", "ARABA/ALAVA"),
        ("AVILA", "AVILA"),
        ("Ávila", "AVILA"),
        ("CACERES", "CACERES"),
        ("Cáceres", "CACERES"),
        ("CADIZ", "CADIZ"),
        ("Cádiz", "CADIZ"),
        ("JAEN", "JAEN"),
        ("Jaén", "JAEN"),
        ("LEON", "LEON"),
        ("León", "LEON"),
        ("MALAGA", "MALAGA"),
        ("Málaga", "MALAGA"),
    ],
)
def test_normalize_geographical_name(
    source,
    expected,
):
    assert normalize_geographical_name(source) == expected


def test_normalize_geographical_name_handles_null_and_empty():
    assert normalize_geographical_name(None) is None
    assert normalize_geographical_name("") is None
    assert normalize_geographical_name("   ") is None


# ============================================================================
# Controlled aliases configuration
# ============================================================================

def test_validated_province_aliases_are_loaded():
    with PROVINCE_ALIASES_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        configured_aliases = json.load(file)

    expected_aliases = {
        normalize_geographical_name(source_name):
        normalize_geographical_name(canonical_name)
        for source_name, canonical_name
        in configured_aliases.items()
    }

    aliases = load_province_aliases()

    assert aliases == expected_aliases


# ============================================================================
# Direct deterministic CNIG matching
#
# Same 10 original cases.
# Spark transformation is executed once by direct_match_rows.
# ============================================================================

@pytest.mark.parametrize(
    (
        "source_province",
        "expected_code",
        "expected_name",
    ),
    [
        ("CORDOBA", "14", "Córdoba"),
        ("ALMERIA", "04", "Almería"),
        ("ARABA/ALAVA", "01", "Araba/Álava"),
        ("AVILA", "05", "Ávila"),
        ("CACERES", "10", "Cáceres"),
        ("CADIZ", "11", "Cádiz"),
        ("JAEN", "23", "Jaén"),
        ("LEON", "24", "León"),
        ("MALAGA", "29", "Málaga"),
        ("MADRID", "28", "Madrid"),
    ],
)
def test_direct_normalized_match_against_cnig(
    direct_match_rows,
    source_province,
    expected_code,
    expected_name,
):
    row = direct_match_rows[
        source_province
    ]

    # Source traceability must be preserved.
    assert row["provincia"] == source_province

    # Canonical geography must come from CNIG.
    assert row["province_code"] == expected_code
    assert row["province_name"] == expected_name

    # Canonical province identifiers must never be NULL
    # for a successfully resolved source province.
    assert row["province_code"] is not None
    assert row["province_name"] is not None


# ============================================================================
# Controlled alias fallback
#
# Same 5 original validated aliases.
# Spark transformation is executed once by alias_match_rows.
# ============================================================================

@pytest.mark.parametrize(
    (
        "source_province",
        "expected_code",
        "expected_name",
    ),
    [
        (
            "ALICANTE",
            "03",
            "Alacant/Alicante",
        ),
        (
            "BALEARES",
            "07",
            "Illes Balears",
        ),
        (
            "CASTELLON",
            "12",
            "Castelló/Castellón",
        ),
        (
            "STA. CRUZ DE TENERIFE",
            "38",
            "Santa Cruz de Tenerife",
        ),
        (
            "VALENCIA",
            "46",
            "València/Valencia",
        ),
    ],
)
def test_controlled_alias_fallback_matches_cnig(
    alias_match_rows,
    source_province,
    expected_code,
    expected_name,
):
    row = alias_match_rows[
        source_province
    ]

    # Original source value must remain untouched.
    assert row["provincia"] == source_province

    # Canonical geography must come from CNIG.
    assert row["province_code"] == expected_code
    assert row["province_name"] == expected_name

    # Alias resolution must finish with real canonical
    # identifiers, never with the alias text itself.
    assert row["province_code"] is not None
    assert row["province_name"] is not None


# ============================================================================
# Autonomous-community enrichment / Open-Meteo style column
# ============================================================================

def test_enrichment_adds_canonical_autonomous_community(
    open_meteo_style_row,
):
    row = open_meteo_style_row

    # Original Open-Meteo source value is preserved.
    assert row["province"] == "VALENCIA"

    # Canonical province.
    assert row["province_code"] == "46"
    assert row["province_name"] == "València/Valencia"

    # Canonical autonomous community.
    assert row["autonomous_community_code"] == "10"

    assert (
        row["autonomous_community_name"]
        == "Comunitat Valenciana"
    )


# ============================================================================
# Unmatched validation
# ============================================================================

def test_unknown_province_remains_unmatched(
    unknown_province_result,
):
    row = unknown_province_result.first()

    assert row["provincia"] == "PROVINCIA_INEXISTENTE"
    assert row["province_code"] is None
    assert row["province_name"] is None


def test_validation_fails_when_province_has_no_cnig_match(
    unknown_province_result,
):
    with pytest.raises(
        ValueError,
        match="Geographical normalization failed",
    ):
        validate_all_provinces_matched(
            unknown_province_result,
            dataset_name="test_dataset",
        )


def test_validation_passes_when_every_province_matches(
    spark,
    cnig_provinces,
):
    source_df = spark.createDataFrame(
        [
            ("station-1", "CORDOBA"),
            ("station-2", "ALICANTE"),
            ("station-3", "BALEARES"),
            ("station-4", "VALENCIA"),
        ],
        [
            "station_id",
            "provincia",
        ],
    )

    result = enrich_with_cnig_province(
        source_df,
        cnig_provinces,
        source_province_column="provincia",
    )

    validate_all_provinces_matched(
        result,
        dataset_name="test_dataset",
    )

    assert result.count() == 4