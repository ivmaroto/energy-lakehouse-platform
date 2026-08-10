from datetime import date
from unittest.mock import Mock

import pytest

from ingestion.aemet.client import AemetClient
from ingestion.common.exceptions import (
    APIResponseError,
    InvalidDateRangeError,
)


def test_get_daily_climatological_values_two_step_request():
    http_client = Mock()

    http_client.get_json.side_effect = [
        {
            "descripcion": "exito",
            "estado": 200,
            "datos": "https://example.test/aemet-data",
        },
        [
            {
                "fecha": "2026-08-01",
                "indicativo": "TEST",
                "tmed": "20.0",
            }
        ],
    ]

    client = AemetClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    result = client.get_daily_climatological_values(
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 2),
        station_id="TEST",
    )

    assert result == [
        {
            "fecha": "2026-08-01",
            "indicativo": "TEST",
            "tmed": "20.0",
        }
    ]

    assert http_client.get_json.call_count == 2

    first_call = http_client.get_json.call_args_list[0]
    second_call = http_client.get_json.call_args_list[1]

    first_url = first_call.args[0]
    first_headers = first_call.kwargs["headers"]

    assert "fechaini/2026-08-01T00:00:00UTC" in first_url
    assert "fechafin/2026-08-02T00:00:00UTC" in first_url
    assert "estacion/TEST" in first_url

    assert first_headers["api_key"] == "test-api-key"

    assert second_call.args[0] == (
        "https://example.test/aemet-data"
    )


def test_missing_data_url_raises_response_error():
    http_client = Mock()

    http_client.get_json.return_value = {
        "descripcion": "response without data URL",
        "estado": 200,
    }

    client = AemetClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    with pytest.raises(APIResponseError):
        client.get_daily_climatological_values(
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 2),
            station_id="TEST",
        )


def test_invalid_metadata_format_raises_response_error():
    http_client = Mock()

    http_client.get_json.return_value = [
        "unexpected",
        "response",
    ]

    client = AemetClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    with pytest.raises(APIResponseError):
        client.get_daily_climatological_values(
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 2),
            station_id="TEST",
        )


def test_invalid_date_range_raises_error():
    client = AemetClient(
        api_key="test-api-key",
        http_client=Mock(),
    )

    with pytest.raises(InvalidDateRangeError):
        client.get_daily_climatological_values(
            start_date=date(2026, 8, 10),
            end_date=date(2026, 8, 1),
            station_id="TEST",
        )


def test_empty_station_id_raises_error():
    client = AemetClient(
        api_key="test-api-key",
        http_client=Mock(),
    )

    with pytest.raises(ValueError):
        client.get_daily_climatological_values(
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 2),
            station_id="",
        )