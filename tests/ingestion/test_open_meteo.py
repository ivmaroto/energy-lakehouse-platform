from datetime import date, datetime, timedelta, timezone

from ingestion.common.exceptions import InvalidDateRangeError
from ingestion.open_meteo.ingest import OpenMeteoIngestion

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

    with pytest.raises(InvalidDateRangeError):
        client.get_historical_weather(
            latitude=43.0,
            longitude=-2.5,
            start_date=date(2026, 8, 10),
            end_date=date(2026, 8, 1),
            hourly_variables=[
                "temperature_2m",
            ],
        )
def test_get_historical_forecast_builds_expected_request():
    http_client = Mock()

    http_client.get_json.return_value = {
        "hourly": {
            "wind_speed_80m": [10.0],
            "wind_speed_120m": [12.0],
        }
    }

    client = OpenMeteoClient(
        http_client=http_client,
    )

    result = client.get_historical_forecast(
        latitude=43.0,
        longitude=-2.5,
        start_date=date(2025, 8, 13),
        end_date=date(2025, 8, 13),
        hourly_variables=[
            "wind_speed_80m",
            "wind_speed_120m",
        ],
        timezone="UTC",
    )

    assert "hourly" in result

    _, kwargs = http_client.get_json.call_args
    params = kwargs["params"]

    assert params["latitude"] == 43.0
    assert params["longitude"] == -2.5
    assert params["start_date"] == "2025-08-13"
    assert params["end_date"] == "2025-08-13"
    assert params["hourly"] == (
        "wind_speed_80m,wind_speed_120m"
    )
    assert params["timezone"] == "UTC"


def test_get_minutely_15_weather_builds_exact_window():
    http_client = Mock()

    http_client.get_json.return_value = {
        "minutely_15": {
            "time": [
                "2026-08-13T10:00",
                "2026-08-13T10:15",
            ]
        }
    }

    client = OpenMeteoClient(
        http_client=http_client,
    )

    client.get_minutely_15_weather(
        latitude=43.0,
        longitude=-2.5,
        start_datetime=datetime(
            2026,
            8,
            13,
            10,
            0,
            tzinfo=timezone.utc,
        ),
        end_datetime=datetime(
            2026,
            8,
            13,
            10,
            15,
            tzinfo=timezone.utc,
        ),
        minutely_15_variables=[
            "temperature_2m",
            "wind_speed_80m",
        ],
        timezone="UTC",
    )

    _, kwargs = http_client.get_json.call_args
    params = kwargs["params"]

    assert params["start_minutely_15"] == (
        "2026-08-13T10:00"
    )
    assert params["end_minutely_15"] == (
        "2026-08-13T10:15"
    )
    assert params["minutely_15"] == (
        "temperature_2m,wind_speed_80m"
    )


def test_get_minutely_15_weather_normalizes_to_utc():
    http_client = Mock()

    http_client.get_json.return_value = {
        "minutely_15": {}
    }

    client = OpenMeteoClient(
        http_client=http_client,
    )

    utc_plus_two = timezone(
        timedelta(hours=2)
    )

    client.get_minutely_15_weather(
        latitude=43.0,
        longitude=-2.5,
        start_datetime=datetime(
            2026,
            8,
            13,
            12,
            0,
            tzinfo=utc_plus_two,
        ),
        end_datetime=datetime(
            2026,
            8,
            13,
            12,
            15,
            tzinfo=utc_plus_two,
        ),
        minutely_15_variables=[
            "temperature_2m",
        ],
    )

    _, kwargs = http_client.get_json.call_args
    params = kwargs["params"]

    assert params["start_minutely_15"] == (
        "2026-08-13T10:00"
    )
    assert params["end_minutely_15"] == (
        "2026-08-13T10:15"
    )


def test_invalid_minutely_15_datetime_range_raises_error():
    client = OpenMeteoClient(
        http_client=Mock(),
    )

    with pytest.raises(InvalidDateRangeError):
        client.get_minutely_15_weather(
            latitude=43.0,
            longitude=-2.5,
            start_datetime=datetime(
                2026,
                8,
                13,
                10,
                15,
                tzinfo=timezone.utc,
            ),
            end_datetime=datetime(
                2026,
                8,
                13,
                10,
                0,
                tzinfo=timezone.utc,
            ),
            minutely_15_variables=[
                "temperature_2m",
            ],
        )


def test_ingest_minutely_15_persists_exact_window():
    client = Mock()
    storage = Mock()

    data = {
        "minutely_15": {
            "time": [
                "2026-08-13T10:00",
                "2026-08-13T10:15",
            ]
        }
    }

    client.get_minutely_15_weather.return_value = data

    storage.save_json.return_value = (
        "bronze/open_meteo/weather_15min/test.json"
    )

    ingestion = OpenMeteoIngestion(
        client=client,
        storage=storage,
    )

    start_datetime = datetime(
        2026,
        8,
        13,
        10,
        0,
        tzinfo=timezone.utc,
    )

    end_datetime = datetime(
        2026,
        8,
        13,
        10,
        15,
        tzinfo=timezone.utc,
    )

    result = ingestion.ingest_minutely_15(
        latitude=43.0,
        longitude=-2.5,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        minutely_15_variables=[
            "temperature_2m",
        ],
    )

    client.get_minutely_15_weather.assert_called_once_with(
        latitude=43.0,
        longitude=-2.5,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        minutely_15_variables=[
            "temperature_2m",
        ],
        timezone="UTC",
    )

    storage.save_json.assert_called_once_with(
        data,
        source="open_meteo",
        dataset="weather_15min",
        ingestion_mode="incremental",
        requested_start_date=(
            "2026-08-13T10:00:00+00:00"
        ),
        requested_end_date=(
            "2026-08-13T10:15:00+00:00"
        ),
        extra_metadata={
            "location_id": None,
            "latitude": 43.0,
            "longitude": -2.5,
        },
    )

    assert result == (
        "bronze/open_meteo/weather_15min/test.json"
    )