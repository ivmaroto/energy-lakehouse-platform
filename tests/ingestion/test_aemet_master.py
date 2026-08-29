import pytest

from ingestion.aemet.master import (
    parse_aemet_coordinate,
)


def test_north_coordinate():
    assert parse_aemet_coordinate(
        "394924N",
        allowed_hemispheres={"N", "S"},
    ) == pytest.approx(
        39 + 49 / 60 + 24 / 3600
    )


def test_east_coordinate():
    assert parse_aemet_coordinate(
        "025309E",
        allowed_hemispheres={"E", "W"},
    ) == pytest.approx(
        2 + 53 / 60 + 9 / 3600
    )


def test_west_coordinate():
    assert parse_aemet_coordinate(
        "034049W",
        allowed_hemispheres={"E", "W"},
    ) == pytest.approx(
        -(3 + 40 / 60 + 49 / 3600)
    )


def test_old_longitude_format_is_rejected():
    with pytest.raises(ValueError):
        parse_aemet_coordinate(
            "0034049W",
            allowed_hemispheres={"E", "W"},
        )


def test_real_aemet_seconds_60_is_supported():
    assert parse_aemet_coordinate(
        "364460N",
        allowed_hemispheres={"N", "S"},
    ) == pytest.approx(
        36 + 45 / 60
    )
