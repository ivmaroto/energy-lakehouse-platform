from datetime import date
from unittest.mock import Mock

import pytest

from ingestion.aemet.client import AemetClient
from ingestion.aemet.ingest import AemetIngestion
from ingestion.aemet.radiation_parser import parse_radiation_data
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

def test_get_current_observations_two_step_request():
    http_client = Mock()

    http_client.get_json.side_effect = [
        {
            "descripcion": "exito",
            "estado": 200,
            "datos": "https://example.test/observations",
        },
        [
            {
                "idema": "TEST",
                "ta": 25.0,
            }
        ],
    ]

    client = AemetClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    result = client.get_current_observations()

    assert result == [
        {
            "idema": "TEST",
            "ta": 25.0,
        }
    ]

    assert http_client.get_json.call_count == 2

    first_call = http_client.get_json.call_args_list[0]
    second_call = http_client.get_json.call_args_list[1]

    assert first_call.args[0].endswith(
        "/observacion/convencional/todas"
    )

    assert first_call.kwargs["headers"]["api_key"] == (
        "test-api-key"
    )

    assert second_call.args[0] == (
        "https://example.test/observations"
    )


def test_get_radiation_data_returns_raw_text():
    http_client = Mock()

    http_client.get_json.return_value = {
        "descripcion": "exito",
        "estado": 200,
        "datos": "https://example.test/radiation",
    }

    response = Mock()
    response.text = (
        '"RADIACION SOLAR"\n'
        '"13-08-26"\n'
        '"Estación";"Indicativo";"Tipo"\n'
    )

    http_client.get.return_value = response

    client = AemetClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    result = client.get_radiation_data()

    assert result == response.text

    http_client.get.assert_called_once_with(
        "https://example.test/radiation"
    )


def test_radiation_parser_normalizes_global_radiation():
    raw_text = (
        '"RADIACION SOLAR"\n'
        '"13-08-26"\n'
        '"Estación";"Indicativo";"Tipo";'
        '"5";"6";"7";"8";"9";"10";"11";"12";'
        '"13";"14";"15";"16";"17";"18";"19";"20";'
        '"SUMA"\n'
        '"Test Station";"TEST";"GL";'
        '"0";"1";"2";"3";"4";"5";"6";"7";'
        '"8";"9";"10";"11";"12";"13";"14";"15";'
        '"120"\n'
    )

    result = parse_radiation_data(raw_text)

    assert len(result) == 16

    first_record = result[0]
    last_record = result[-1]

    assert first_record == {
        "station_name": "Test Station",
        "station_id": "TEST",
        "observation_date": "2026-08-13",
        "radiation_type": "GL",
        "solar_time": "05:00",
        "value": 0.0,
        "unit": "10*kJ/m2",
        "daily_total": 120.0,
        "temporal_granularity": "1h",
    }

    assert last_record["solar_time"] == "20:00"
    assert last_record["value"] == 15.0


def test_radiation_parser_rejects_empty_dataset():
    with pytest.raises(ValueError):
        parse_radiation_data("")


def test_ingest_radiation_persists_raw_csv():
    client = Mock()
    storage = Mock()

    raw_data = (
        '"RADIACION SOLAR"\n'
        '"13-08-26"\n'
    )

    client.get_radiation_data.return_value = raw_data

    storage.save_text.return_value = (
        "bronze/aemet/radiation/test.csv"
    )

    ingestion = AemetIngestion(
        client=client,
        storage=storage,
    )

    result = ingestion.ingest_radiation()

    client.get_radiation_data.assert_called_once_with()

    storage.save_text.assert_called_once_with(
        raw_data,
        source="aemet",
        dataset="radiation",
        ingestion_mode="incremental",
        extension="csv",
        content_type="text/csv",
    )

    assert result == (
        "bronze/aemet/radiation/test.csv"
    )


def test_ingest_current_observations_persists_json():
    client = Mock()
    storage = Mock()

    observations = [
        {
            "idema": "TEST",
            "ta": 25.0,
        }
    ]

    client.get_current_observations.return_value = (
        observations
    )

    storage.save_json.return_value = (
        "bronze/aemet/current_observations/test.json"
    )

    ingestion = AemetIngestion(
        client=client,
        storage=storage,
    )

    result = ingestion.ingest_current_observations()

    client.get_current_observations.assert_called_once_with()

    storage.save_json.assert_called_once_with(
        observations,
        source="aemet",
        dataset="current_observations",
        ingestion_mode="incremental",
    )

    assert result == (
        "bronze/aemet/current_observations/test.json"
    )