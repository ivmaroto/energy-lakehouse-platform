from datetime import date
from unittest.mock import Mock

import pytest

from ingestion.aemet.client import AemetClient
from ingestion.aemet.ingest import AemetIngestion
from ingestion.common.exceptions import (
    APIResponseError,
    InvalidDateRangeError,
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


def test_ingest_current_observations_splits_by_observation_day():
    client = Mock()
    storage = Mock()

    observations = [
        {
            "idema": "STA",
            "fint": (
                "2026-08-29T23:00:00+0000"
            ),
            "ta": 20.0,
        },
        {
            "idema": "STA",
            "fint": (
                "2026-08-30T00:00:00+0000"
            ),
            "ta": 19.0,
        },
    ]

    client.get_current_observations.return_value = (
        observations
    )

    storage.object_exists.return_value = (
        False
    )

    storage.save_json.side_effect = [
        (
            "bronze/aemet/current_observations/"
            "year=2026/month=08/day=29/"
            "observations.json"
        ),
        (
            "bronze/aemet/current_observations/"
            "year=2026/month=08/day=30/"
            "observations.json"
        ),
    ]

    ingestion = AemetIngestion(
        client=client,
        storage=storage,
    )

    result = (
        ingestion
        .ingest_current_observations()
    )

    assert len(result) == 2

    assert (
        storage.save_json.call_count
        == 2
    )

    first = (
        storage.save_json
        .call_args_list[0]
    )

    second = (
        storage.save_json
        .call_args_list[1]
    )

    assert first.kwargs[
        "object_name"
    ] == (
        "bronze/aemet/current_observations/"
        "year=2026/month=08/day=29/"
        "observations.json"
    )

    assert second.kwargs[
        "object_name"
    ] == (
        "bronze/aemet/current_observations/"
        "year=2026/month=08/day=30/"
        "observations.json"
    )


def test_ingest_current_observations_merges_without_duplicates():
    client = Mock()
    storage = Mock()

    object_name = (
        "bronze/aemet/current_observations/"
        "year=2026/month=08/day=30/"
        "observations.json"
    )

    existing_record = {
        "idema": "STA",
        "fint": (
            "2026-08-30T07:00:00+0000"
        ),
        "ta": 18.0,
    }

    updated_record = {
        "idema": "STA",
        "fint": (
            "2026-08-30T07:00:00+0000"
        ),
        "ta": 18.5,
    }

    new_record = {
        "idema": "STA",
        "fint": (
            "2026-08-30T08:00:00+0000"
        ),
        "ta": 19.0,
    }

    client.get_current_observations.return_value = [
        updated_record,
        new_record,
    ]

    storage.object_exists.return_value = (
        True
    )

    storage.read_json.return_value = {
        "metadata": {},
        "data": [
            existing_record,
        ],
    }

    storage.save_json.return_value = (
        object_name
    )

    ingestion = AemetIngestion(
        client=client,
        storage=storage,
    )

    result = (
        ingestion
        .ingest_current_observations()
    )

    assert result == [
        object_name
    ]

    storage.read_json.assert_called_once_with(
        object_name
    )

    saved_data = (
        storage.save_json
        .call_args.args[0]
    )

    assert len(saved_data) == 2

    assert updated_record in (
        saved_data
    )

    assert new_record in (
        saved_data
    )

    assert existing_record not in (
        saved_data
    )


def test_ingest_stations_uses_canonical_master_path():
    client = Mock()
    storage = Mock()

    stations = [
        {
            "indicativo": "TEST",
            "nombre": "Test station",
        }
    ]

    client.get_stations.return_value = (
        stations
    )

    expected_path = (
        "bronze/aemet/stations/"
        "stations.json"
    )

    storage.save_json.return_value = (
        expected_path
    )

    ingestion = AemetIngestion(
        client=client,
        storage=storage,
    )

    result = ingestion.ingest_stations()

    client.get_stations.assert_called_once_with()

    storage.save_json.assert_called_once_with(
        stations,
        source="aemet",
        dataset="stations",
        object_name=expected_path,
        ingestion_mode="snapshot",
    )

    assert result == expected_path