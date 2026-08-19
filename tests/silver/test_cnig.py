import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.cnig import (
    transform_autonomous_communities,
    transform_municipalities,
    transform_provinces,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-cnig-tests")
        .getOrCreate()
    )

    yield session
    session.stop()


def test_transform_provinces(spark):
    df = spark.createDataFrame(
        [
            (
                "01",
                "Araba/Álava",
                "16",
                "País Vasco/Euskadi",
                "Vitoria-Gasteiz",
            ),
            (
                "02",
                "Albacete",
                "08",
                "Castilla-La Mancha",
                "Albacete",
            ),
        ],
        [
            "COD_PROV",
            "PROVINCIA",
            "COD_CA",
            "COMUNIDAD_AUTONOMA",
            "CAPITAL",
        ],
    )

    result = transform_provinces(df)

    assert result.count() == 2

    rows = {
        row["province_code"]: row
        for row in result.collect()
    }

    assert rows["01"]["province_name"] == "Araba/Álava"
    assert rows["01"]["autonomous_community_code"] == "16"
    assert (
        rows["01"]["autonomous_community_name"]
        == "País Vasco/Euskadi"
    )
    assert rows["01"]["capital_name"] == "Vitoria-Gasteiz"
    assert rows["01"]["source"] == "cnig"


def test_transform_autonomous_communities_deduplicates(spark):
    df = spark.createDataFrame(
        [
            (
                "01",
                "Province A",
                "08",
                "Castilla-La Mancha",
                "Capital A",
            ),
            (
                "02",
                "Province B",
                "08",
                "Castilla-La Mancha",
                "Capital B",
            ),
            (
                "03",
                "Province C",
                "10",
                "Comunitat Valenciana",
                "Capital C",
            ),
        ],
        [
            "COD_PROV",
            "PROVINCIA",
            "COD_CA",
            "COMUNIDAD_AUTONOMA",
            "CAPITAL",
        ],
    )

    provinces = transform_provinces(df)
    result = transform_autonomous_communities(provinces)

    assert result.count() == 2

    codes = {
        row["autonomous_community_code"]
        for row in result.collect()
    }

    assert codes == {"08", "10"}


def test_transform_municipalities_preserves_codes(spark):
    df = spark.createDataFrame(
        [
            (
                "01001000000",
                "1010014",
                "01010",
                "01",
                "Araba/Álava",
                "Alegría-Dulantzi",
                "2961",
                "1994,5872",
                "35069",
                "01001000101",
                "Alegría-Dulantzi",
                "2842",
                "0113-3",
                "-2,512507724",
                "42,84045247",
                "Detección automática",
                "568",
                "MDT",
            ),
        ],
        [
            "COD_INE",
            "ID_REL",
            "COD_GEO",
            "COD_PROV",
            "PROVINCIA",
            "NOMBRE_ACTUAL",
            "POBLACION_MUNI",
            "SUPERFICIE",
            "PERIMETRO",
            "COD_INE_CAPITAL",
            "CAPITAL",
            "POBLACION_CAPITAL",
            "HOJA_MTN25",
            "LONGITUD_ETRS89_REGCAN95",
            "LATITUD_ETRS89_REGCAN95",
            "ORIGENCOOR",
            "ALTITUD",
            "ORIGENALTITUD",
        ],
    )

    result = transform_municipalities(df)
    row = result.first()

    assert row["municipality_ine_code"] == "01001000000"
    assert row["municipality_code"] == "01010"
    assert row["province_code"] == "01"

    assert row["municipality_population"] == 2961
    assert row["surface_area"] == pytest.approx(1994.5872)
    assert row["perimeter"] == 35069

    assert row["longitude"] == pytest.approx(-2.512507724)
    assert row["latitude"] == pytest.approx(42.84045247)
    assert row["altitude"] == pytest.approx(568.0)

    assert row["source"] == "cnig"


def test_municipality_natural_key_is_cod_ine(spark):
    df = spark.createDataFrame(
        [
            (
                "11903000000",
                "1119034",
                "00000",
                "11",
                "Cádiz",
                "San Martín del Tesorillo",
                "1",
                "1,0",
                "1",
                "11903000001",
                "Capital A",
                "1",
                "0000-0",
                "-5,0",
                "36,0",
                "Test",
                "1",
                "Test",
            ),
            (
                "14901000000",
                "1149019",
                "00000",
                "14",
                "Córdoba",
                "Fuente Carreteros",
                "1",
                "1,0",
                "1",
                "14901000001",
                "Capital B",
                "1",
                "0000-0",
                "-4,0",
                "37,0",
                "Test",
                "1",
                "Test",
            ),
        ],
        [
            "COD_INE",
            "ID_REL",
            "COD_GEO",
            "COD_PROV",
            "PROVINCIA",
            "NOMBRE_ACTUAL",
            "POBLACION_MUNI",
            "SUPERFICIE",
            "PERIMETRO",
            "COD_INE_CAPITAL",
            "CAPITAL",
            "POBLACION_CAPITAL",
            "HOJA_MTN25",
            "LONGITUD_ETRS89_REGCAN95",
            "LATITUD_ETRS89_REGCAN95",
            "ORIGENCOOR",
            "ALTITUD",
            "ORIGENALTITUD",
        ],
    )

    result = transform_municipalities(df)

    # COD_GEO is intentionally duplicated, but both municipalities
    # must survive because COD_INE is the approved natural key.
    assert result.count() == 2

    assert (
        result.select("municipality_ine_code")
        .distinct()
        .count()
        == 2
    )

    assert (
        result.filter(F.col("municipality_code") == "00000")
        .count()
        == 2
    )


def test_missing_required_cnig_column_fails(spark):
    df = spark.createDataFrame(
        [
            ("01", "Araba/Álava"),
        ],
        [
            "COD_PROV",
            "PROVINCIA",
        ],
    )

    with pytest.raises(ValueError):
        transform_provinces(df)