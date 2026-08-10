from datetime import date
from unittest.mock import Mock

import pytest

from ingestion.open_meteo.client import OpenMeteoClient


def test_get_historical_weather_builds_expected_request():
    http_client = Mock()

    http_client.get_json.return_value = {
        "hourly": {
            "temperature_2m": [20.0, 21.0],
        }
    }

    client = OpenMeteoClient(
        http_client=http_client,
    )

    result = client.get_historical_weather(
        latitude=43.0,
        longitude=-2.5,
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 2),
        hourly_variables=[
            "temperature_2m",
            "precipitation",
        ],
        timezone="UTC",
    )

    assert result == {
        "hourly": {
            "temperature_2m": [20.0, 21.0],
        }
    }

    http_client.get_json.assert_called_once()

    _, kwargs = http_client.get_json.call_args

    assert kwargs["params"]["latitude"] == 43.0
    assert kwargs["params"]["longitude"] == -2.5
    assert kwargs["params"]["start_date"] == "2026-08-01"
    assert kwargs["params"]["end_date"] == "2026-08-02"
    assert kwargs["params"]["hourly"] == (
        "temperature_2m,precipitation"
    )
    assert kwargs["params"]["timezone"] == "UTC"


def test_get_current_weather_builds_expected_request():
    http_client = Mock()

    http_client.get_json.return_value = {
        "current": {
            "temperature_2m": 24.0,
        }
    }

    client = OpenMeteoClient(
        http_client=http_client,
    )

    result = client.get_current_weather(
        latitude=43.0,
        longitude=-2.5,
        current_variables=[
            "temperature_2m",
        ],
        timezone="UTC",
    )

    assert result["current"]["temperature_2m"] == 24.0

    http_client.get_json.assert_called_once()

    _, kwargs = http_client.get_json.call_args

    assert kwargs["params"]["latitude"] == 43.0
    assert kwargs["params"]["longitude"] == -2.5
    assert kwargs["params"]["current"] == "temperature_2m"
    assert kwargs["params"]["timezone"] == "UTC"


@pytest.mark.parametrize(
    "latitude",
    [-90.1, 90.1],
)
def test_invalid_latitude_raises_error(latitude):
    client = OpenMeteoClient(
        http_client=Mock(),
    )

    with pytest.raises(ValueError):
        client.get_current_weather(
            latitude=latitude,
            longitude=-2.5,
            current_variables=[
                "temperature_2m",
            ],
        )


@pytest.mark.parametrize(
    "longitude",
    [-180.1, 180.1],
)
def test_invalid_longitude_raises_error(longitude):
    client = OpenMeteoClient(
        http_client=Mock(),
    )

    with pytest.raises(ValueError):
        client.get_current_weather(
            latitude=43.0,
            longitude=longitude,
            current_variables=[
                "temperature_2m",
            ],
        )


def test_invalid_historical_date_range_raises_error():
    client = OpenMeteoClient(
        http_client=Mock(),
    )

    with pytest.raises(Exception):
        client.get_historical_weather(
            latitude=43.0,
            longitude=-2.5,
            start_date=date(2026, 8, 10),
            end_date=date(2026, 8, 1),
            hourly_variables=[
                "temperature_2m",
            ],
        )