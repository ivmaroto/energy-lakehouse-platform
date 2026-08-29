import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from silver.aemet import (
    transform_current_observations,
    transform_stations,
)


CURRENT_OBSERVATIONS_COLUMNS = [
    "alt",
    "dmax",
    "dmaxu",
    "dv",
    "dvu",
    "fint",
    "geo700",
    "geo850",
    "geo925",
    "hr",
    "idema",
    "inso",
    "lat",
    "lon",
    "nieve",
    "pacutp",
    "pliqt",
    "prec",
    "pres",
    "pres_nmar",
    "psoltp",
    "rviento",
    "stddv",
    "stddvu",
    "stdvv",
    "stdvvu",
    "ta",
    "tamax",
    "tamin",
    "tpr",
    "ts",
    "tss20cm",
    "tss5cm",
    "ubi",
    "vis",
    "vmax",
    "vmaxu",
    "vv",
    "vvu",
]


CURRENT_OBSERVATIONS_SCHEMA = StructType(
    [
        StructField(
            column_name,
            StringType(),
            True,
        )
        for column_name in CURRENT_OBSERVATIONS_COLUMNS
    ]
)


CURRENT_OBSERVATION_ROW = (
    "667.0",                  # alt
    "180.0",                  # dmax
    "180.0",                  # dmaxu
    "170.0",                  # dv
    "170.0",                  # dvu
    "2026-08-18T14:00:00",    # fint
    None,                     # geo700
    None,                     # geo850
    None,                     # geo925
    "45.0",                   # hr
    "3195",                   # idema
    None,                     # inso
    "40.411",                 # lat
    "-3.678",                 # lon
    None,                     # nieve
    None,                     # pacutp
    None,                     # pliqt
    "0.0",                    # prec
    "940.0",                  # pres
    "1012.0",                 # pres_nmar
    None,                     # psoltp
    None,                     # rviento
    None,                     # stddv
    None,                     # stddvu
    None,                     # stdvv
    None,                     # stdvvu
    "31.2",                   # ta
    "32.0",                   # tamax
    "21.1",                   # tamin
    "15.0",                   # tpr
    None,                     # ts
    None,                     # tss20cm
    None,                     # tss5cm
    "MADRID, RETIRO",         # ubi
    "20000.0",                # vis
    "14.0",                   # vmax
    "14.0",                   # vmaxu
    "3.8",                    # vv
    "3.8",                    # vvu
)


DAILY_ROW = (
    "667",
    "180",
    "2026-08-17",
    "14:00",
    "06:00",
    "12:30",
    "15:00",
    "05:00",
    "15:10",
    "15:00",
    "05:30",
    "80",
    "55",
    "30",
    "3195",
    "MADRID, RETIRO",
    "0,0",
    "0,0",
    "950,2",
    "940,1",
    "MADRID",
    "12,5",
    "10,2",
    "32,1",
    "24,3",
    "16,5",
    "3,4",
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-aemet-tests")
        .getOrCreate()
    )

    yield session
    session.stop()


def test_transform_stations(spark):
    df = spark.createDataFrame(
        [
            (
                "3195",
                "MADRID, RETIRO",
                "MADRID",
                "667",
                "402443N",
                "034049W",
                "08222",
            ),
        ],
        [
            "indicativo",
            "nombre",
            "provincia",
            "altitud",
            "latitud",
            "longitud",
            "indsinop",
        ],
    )

    result = transform_stations(df)
    row = result.first()

    assert result.count() == 1

    assert row["station_id"] == "3195"
    assert row["nombre"] == "MADRID, RETIRO"
    assert row["provincia"] == "MADRID"
    assert row["altitud"] == pytest.approx(667.0)

    assert row["latitude"] == pytest.approx(
        40 + 24 / 60 + 43 / 3600
    )

    assert row["longitude"] == pytest.approx(
        -(3 + 40 / 60 + 49 / 3600)
    )

    assert row["indsinop"] == "08222"
    assert row["source"] == "aemet"


def test_transform_stations_deduplicates_by_station_id(spark):
    df = spark.createDataFrame(
        [
            (
                "3195",
                "MADRID, RETIRO",
                "MADRID",
                "667",
                "402443N",
                "034049W",
                "08222",
            ),
            (
                "3195",
                "MADRID, RETIRO",
                "MADRID",
                "667",
                "402443N",
                "034049W",
                "08222",
            ),
        ],
        [
            "indicativo",
            "nombre",
            "provincia",
            "altitud",
            "latitud",
            "longitud",
            "indsinop",
        ],
    )

    result = transform_stations(df)

    assert result.count() == 1






def test_transform_current_observations(spark):
    df = spark.createDataFrame(
        [
            CURRENT_OBSERVATION_ROW,
        ],
        schema=CURRENT_OBSERVATIONS_SCHEMA,
    )

    result = transform_current_observations(df)
    row = result.first()

    assert result.count() == 1

    assert row["station_id"] == "3195"
    assert row["observation_timestamp"] is not None

    assert row["latitude"] == pytest.approx(40.411)
    assert row["longitude"] == pytest.approx(-3.678)

    assert row["ta"] == "31.2"
    assert row["hr"] == "45.0"
    assert row["prec"] == "0.0"

    assert row["source"] == "aemet"


def test_current_observations_natural_key_deduplicates(spark):
    df = spark.createDataFrame(
        [
            CURRENT_OBSERVATION_ROW,
            CURRENT_OBSERVATION_ROW,
        ],
        schema=CURRENT_OBSERVATIONS_SCHEMA,
    )

    result = transform_current_observations(df)

    assert result.count() == 1


def test_missing_required_aemet_column_fails(spark):
    df = spark.createDataFrame(
        [
            (
                "3195",
                "MADRID",
            ),
        ],
        [
            "indicativo",
            "nombre",
        ],
    )

    with pytest.raises(ValueError):
        transform_stations(df)