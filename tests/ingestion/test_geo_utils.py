import pytest

from ingestion.common.geo_utils import aemet_coordinate_to_decimal


def test_convert_north_coordinate() -> None:
    result = aemet_coordinate_to_decimal("394924N")

    assert result == pytest.approx(39.82333333333334)


def test_convert_south_coordinate() -> None:
    result = aemet_coordinate_to_decimal("394924S")

    assert result == pytest.approx(-39.82333333333334)


def test_convert_east_coordinate() -> None:
    result = aemet_coordinate_to_decimal("025309E")

    assert result == pytest.approx(2.8858333333333333)


def test_convert_west_coordinate() -> None:
    result = aemet_coordinate_to_decimal("025309W")

    assert result == pytest.approx(-2.8858333333333333)


def test_convert_coordinate_with_sixty_seconds() -> None:
    result = aemet_coordinate_to_decimal("364460N")

    assert result == pytest.approx(36.75)


def test_empty_coordinate_raises_value_error() -> None:
    with pytest.raises(ValueError):
        aemet_coordinate_to_decimal("")


def test_invalid_hemisphere_raises_value_error() -> None:
    with pytest.raises(ValueError):
        aemet_coordinate_to_decimal("394924X")


def test_invalid_coordinate_format_raises_value_error() -> None:
    with pytest.raises(ValueError):
        aemet_coordinate_to_decimal("3949N")


def test_invalid_minutes_raises_value_error() -> None:
    with pytest.raises(ValueError):
        aemet_coordinate_to_decimal("396024N")


def test_invalid_seconds_over_sixty_raises_value_error() -> None:
    with pytest.raises(ValueError):
        aemet_coordinate_to_decimal("394961N")