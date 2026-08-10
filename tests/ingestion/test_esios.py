from datetime import date
from unittest.mock import Mock

import pytest

from ingestion.common.exceptions import InvalidDateRangeError
from ingestion.esios.client import EsiosClient


def test_get_indicators_builds_expected_request():
    http_client = Mock()

    http_client.get_json.return_value = {
        "indicators": [
            {
                "id": 1,
                "name": "Test indicator",
            }
        ]
    }

    client = EsiosClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    result = client.get_indicators()

    assert "indicators" in result

    http_client.get_json.assert_called_once()

    call = http_client.get_json.call_args

    assert call.args[0].endswith("/indicators")
    assert call.kwargs["headers"]["x-api-key"] == "test-api-key"


def test_get_indicator_builds_expected_request():
    http_client = Mock()

    http_client.get_json.return_value = {
        "indicator": {
            "id": 123,
            "values": [
                {
                    "value": 100.5,
                }
            ],
        }
    }

    client = EsiosClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    result = client.get_indicator(
        indicator_id=123,
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 2),
        time_trunc="day",
        time_agg="sum",
        geo_ids=[1, 2],
        geo_trunc="autonomous_community",
        geo_agg="sum",
    )

    assert result["indicator"]["id"] == 123

    http_client.get_json.assert_called_once()

    call = http_client.get_json.call_args

    assert call.args[0].endswith("/indicators/123")

    params = call.kwargs["params"]

    assert params["start_date"] == "2026-08-01T00:00:00Z"
    assert params["end_date"] == "2026-08-02T23:59:59Z"
    assert params["time_trunc"] == "day"
    assert params["time_agg"] == "sum"
    assert params["geo_ids[]"] == [1, 2]
    assert params["geo_trunc"] == "autonomous_community"
    assert params["geo_agg"] == "sum"

    headers = call.kwargs["headers"]

    assert headers["x-api-key"] == "test-api-key"


def test_optional_parameters_are_not_sent_when_missing():
    http_client = Mock()

    http_client.get_json.return_value = {
        "indicator": {
            "id": 123,
        }
    }

    client = EsiosClient(
        api_key="test-api-key",
        http_client=http_client,
    )

    client.get_indicator(
        indicator_id=123,
        start_date=date(2026, 8, 1),
        end_date=date(2026, 8, 2),
    )

    call = http_client.get_json.call_args

    params = call.kwargs["params"]

    assert "time_trunc" not in params
    assert "time_agg" not in params
    assert "geo_ids[]" not in params
    assert "geo_trunc" not in params
    assert "geo_agg" not in params


def test_invalid_date_range_raises_error():
    client = EsiosClient(
        api_key="test-api-key",
        http_client=Mock(),
    )

    with pytest.raises(InvalidDateRangeError):
        client.get_indicator(
            indicator_id=123,
            start_date=date(2026, 8, 10),
            end_date=date(2026, 8, 1),
        )


@pytest.mark.parametrize(
    "indicator_id",
    [0, -1, -100],
)
def test_invalid_indicator_id_raises_error(indicator_id):
    client = EsiosClient(
        api_key="test-api-key",
        http_client=Mock(),
    )

    with pytest.raises(ValueError):
        client.get_indicator(
            indicator_id=indicator_id,
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 2),
        )