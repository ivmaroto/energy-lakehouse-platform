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