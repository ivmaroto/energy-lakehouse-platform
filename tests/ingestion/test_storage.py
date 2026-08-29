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


def test_save_json_uses_bronze_object_structure(
    monkeypatch,
):
    storage, client = build_storage(monkeypatch)

    object_name = storage.save_json(
        {"value": 123},
        source="esios",
        dataset="generation",
        ingestion_mode="historical",
    )

    assert object_name.startswith(
        "bronze/esios/generation/year="
    )
    assert object_name.endswith(".json")

    kwargs = client.put_object.call_args.kwargs

    assert kwargs["bucket_name"] == "energy-lakehouse"
    assert kwargs["object_name"] == object_name
    assert kwargs["content_type"] == "application/json"


def test_save_json_contains_metadata_and_data(
    monkeypatch,
):
    storage, client = build_storage(monkeypatch)

    storage.save_json(
        [{"value": 1}, {"value": 2}],
        source="aemet",
        dataset="stations",
        ingestion_mode="incremental",
        requested_start_date="2026-08-09",
        requested_end_date="2026-08-10",
    )

    stream = client.put_object.call_args.kwargs["data"]
    payload = json.loads(
        stream.getvalue().decode("utf-8")
    )

    assert payload["data"] == [
        {"value": 1},
        {"value": 2},
    ]

    metadata = payload["metadata"]

    assert metadata["source"] == "aemet"
    assert metadata["dataset"] == (
        "stations"
    )
    assert metadata["ingestion_mode"] == "incremental"
    assert metadata["requested_start_date"] == (
        "2026-08-09"
    )
    assert metadata["requested_end_date"] == (
        "2026-08-10"
    )
    assert metadata["ingestion_timestamp"]


def test_save_json_generates_unique_objects(
    monkeypatch,
):
    storage, _ = build_storage(monkeypatch)

    first = storage.save_json(
        {"value": 1},
        source="open_meteo",
        dataset="weather_hourly",
        ingestion_mode="historical",
    )

    second = storage.save_json(
        {"value": 2},
        source="open_meteo",
        dataset="weather_hourly",
        ingestion_mode="historical",
    )

    assert first != second


def test_save_bytes_preserves_raw_payload(
    monkeypatch,
):
    storage, client = build_storage(monkeypatch)

    raw_data = b"COD_PROV;PROVINCIA\n01;Araba"

    object_name = storage.save_bytes(
        raw_data,
        source="cnig",
        dataset="provinces",
        extension="csv",
        content_type="text/csv",
    )

    kwargs = client.put_object.call_args.kwargs
    stream = kwargs["data"]

    assert object_name.startswith(
        "bronze/cnig/provinces/year="
    )
    assert stream.getvalue() == raw_data
    assert kwargs["content_type"] == "text/csv"


def test_missing_bucket_raises_storage_error(
    monkeypatch,
):
    storage, client = build_storage(monkeypatch)
    client.bucket_exists.return_value = False

    with pytest.raises(StorageError):
        storage.save_json(
            {"value": 1},
            source="aemet",
            dataset="stations",
            ingestion_mode="snapshot",
        )
