import json
from unittest.mock import Mock

import pytest

from ingestion.common.exceptions import StorageError
from ingestion.common.storage import MinIOBronzeStorage


def build_storage(monkeypatch):
    client = Mock()
    client.bucket_exists.return_value = True

    monkeypatch.setattr(
        "ingestion.common.storage.Minio",
        lambda *args, **kwargs: client,
    )

    storage = MinIOBronzeStorage(
        endpoint="minio:9000",
        access_key="test-access",
        secret_key="test-secret",
        bucket="energy-lakehouse",
        secure=False,
    )

    return storage, client


def test_save_json_uses_explicit_object_name(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    expected = (
        "bronze/esios/generation/"
        "year=2026/month=07/day=10/data.json"
    )

    object_name = storage.save_json(
        {"value": 123},
        source="esios",
        dataset="generation",
        object_name=expected,
        ingestion_mode="historical",
    )

    assert object_name == expected

    kwargs = (
        client.put_object
        .call_args.kwargs
    )

    assert kwargs["bucket_name"] == (
        "energy-lakehouse"
    )

    assert kwargs["object_name"] == (
        expected
    )

    assert kwargs["content_type"] == (
        "application/json"
    )


def test_save_json_contains_metadata_and_data(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    storage.save_json(
        [
            {"value": 1},
            {"value": 2},
        ],
        source="open_meteo",
        dataset="weather_hourly",
        object_name=(
            "bronze/open_meteo/"
            "weather_hourly/"
            "year=2026/month=07/day=10/"
            "station_id=0002I.json"
        ),
        ingestion_mode="historical",
        requested_start_date=(
            "2026-07-10"
        ),
        requested_end_date=(
            "2026-07-10"
        ),
    )

    stream = (
        client.put_object
        .call_args.kwargs["data"]
    )

    payload = json.loads(
        stream.getvalue().decode(
            "utf-8"
        )
    )

    assert payload["data"] == [
        {"value": 1},
        {"value": 2},
    ]

    metadata = payload[
        "metadata"
    ]

    assert metadata["source"] == (
        "open_meteo"
    )

    assert metadata["dataset"] == (
        "weather_hourly"
    )

    assert metadata[
        "ingestion_mode"
    ] == "historical"

    assert metadata[
        "requested_start_date"
    ] == "2026-07-10"

    assert metadata[
        "requested_end_date"
    ] == "2026-07-10"

    assert metadata[
        "ingestion_timestamp"
    ]


def test_same_object_name_is_deterministic(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    object_name = (
        "bronze/open_meteo/"
        "weather_hourly/"
        "year=2026/month=07/day=10/"
        "station_id=0002I.json"
    )

    first = storage.save_json(
        {"value": 1},
        source="open_meteo",
        dataset="weather_hourly",
        object_name=object_name,
        ingestion_mode="historical",
    )

    second = storage.save_json(
        {"value": 2},
        source="open_meteo",
        dataset="weather_hourly",
        object_name=object_name,
        ingestion_mode="historical",
    )

    assert first == second

    assert (
        client.put_object.call_count
        == 2
    )


def test_save_bytes_preserves_raw_payload(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    raw_data = (
        b"COD_PROV;PROVINCIA\n"
        b"01;Araba"
    )

    expected = (
        "bronze/cnig/provinces/"
        "provinces.csv"
    )

    object_name = storage.save_bytes(
        raw_data,
        source="cnig",
        dataset="provinces",
        object_name=expected,
        content_type="text/csv",
    )

    kwargs = (
        client.put_object
        .call_args.kwargs
    )

    stream = kwargs["data"]

    assert object_name == expected
    assert stream.getvalue() == raw_data

    assert kwargs[
        "content_type"
    ] == "text/csv"


def test_invalid_bronze_object_name_fails(
    monkeypatch,
):
    storage, _ = build_storage(
        monkeypatch
    )

    with pytest.raises(
        ValueError
    ):
        storage.save_json(
            {"value": 1},
            source="aemet",
            dataset="stations",
            object_name=(
                "aemet/stations.json"
            ),
            ingestion_mode="snapshot",
        )


def test_missing_bucket_raises_storage_error(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    client.bucket_exists.return_value = (
        False
    )

    with pytest.raises(
        StorageError
    ):
        storage.save_json(
            {"value": 1},
            source="aemet",
            dataset="stations",
            object_name=(
                "bronze/aemet/stations/"
                "stations.json"
            ),
            ingestion_mode="snapshot",
        )


def test_object_exists_requires_exact_object(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    object_name = (
        "bronze/aemet/current_observations/"
        "year=2026/month=08/day=30/"
        "observations.json"
    )

    exact = Mock()
    exact.object_name = object_name

    client.list_objects.return_value = [
        exact
    ]

    assert storage.object_exists(
        object_name
    ) is True


def test_object_exists_returns_false_when_missing(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    client.list_objects.return_value = []

    assert storage.object_exists(
        "bronze/aemet/current_observations/"
        "year=2026/month=08/day=30/"
        "observations.json"
    ) is False


def test_read_json_returns_existing_payload(
    monkeypatch,
):
    storage, client = build_storage(
        monkeypatch
    )

    object_name = (
        "bronze/aemet/current_observations/"
        "year=2026/month=08/day=30/"
        "observations.json"
    )

    payload = {
        "metadata": {
            "source": "aemet",
        },
        "data": [
            {
                "idema": "TEST",
                "fint": (
                    "2026-08-30T08:00:00+0000"
                ),
            }
        ],
    }

    response = Mock()
    response.read.return_value = (
        json.dumps(
            payload
        ).encode(
            "utf-8"
        )
    )

    client.get_object.return_value = (
        response
    )

    result = storage.read_json(
        object_name
    )

    assert result == payload

    client.get_object.assert_called_once_with(
        "energy-lakehouse",
        object_name,
    )

    response.close.assert_called_once_with()
    response.release_conn.assert_called_once_with()