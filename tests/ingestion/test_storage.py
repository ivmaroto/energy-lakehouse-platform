import json
from pathlib import Path

from ingestion.common.storage import LocalBronzeStorage


def test_save_json_creates_bronze_file(tmp_path):
    storage = LocalBronzeStorage(base_path=tmp_path)

    data = {
        "temperature": 21.5,
        "humidity": 55,
    }

    output_path = storage.save_json(
        data,
        source="open_meteo",
        dataset="weather",
        ingestion_mode="historical",
        requested_start_date="2026-08-01",
        requested_end_date="2026-08-02",
    )

    assert output_path.exists()
    assert output_path.is_file()


def test_save_json_creates_expected_directory_structure(tmp_path):
    storage = LocalBronzeStorage(base_path=tmp_path)

    output_path = storage.save_json(
        {"value": 123},
        source="esios",
        dataset="generation",
        ingestion_mode="historical",
    )

    relative_path = output_path.relative_to(tmp_path)

    parts = relative_path.parts

    assert parts[0] == "esios"
    assert parts[1] == "generation"
    assert parts[2].startswith("year=")
    assert parts[3].startswith("month=")
    assert parts[4].startswith("day=")


def test_save_json_contains_metadata_and_data(tmp_path):
    storage = LocalBronzeStorage(base_path=tmp_path)

    original_data = [
        {"value": 1},
        {"value": 2},
    ]

    output_path = storage.save_json(
        original_data,
        source="aemet",
        dataset="daily_climatological_values",
        ingestion_mode="incremental",
        requested_start_date="2026-08-09",
        requested_end_date="2026-08-10",
    )

    with output_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        payload = json.load(file)

    assert "metadata" in payload
    assert "data" in payload

    assert payload["data"] == original_data

    metadata = payload["metadata"]

    assert metadata["source"] == "aemet"
    assert metadata["dataset"] == "daily_climatological_values"
    assert metadata["ingestion_mode"] == "incremental"
    assert metadata["requested_start_date"] == "2026-08-09"
    assert metadata["requested_end_date"] == "2026-08-10"
    assert metadata["ingestion_timestamp"]


def test_save_json_generates_unique_files(tmp_path):
    storage = LocalBronzeStorage(base_path=tmp_path)

    first_path = storage.save_json(
        {"value": 1},
        source="open_meteo",
        dataset="weather",
        ingestion_mode="incremental",
    )

    second_path = storage.save_json(
        {"value": 2},
        source="open_meteo",
        dataset="weather",
        ingestion_mode="incremental",
    )

    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()


def test_generated_file_is_valid_json(tmp_path):
    storage = LocalBronzeStorage(base_path=tmp_path)

    output_path = storage.save_json(
        {"test": True},
        source="open_meteo",
        dataset="weather",
        ingestion_mode="historical",
    )

    content = output_path.read_text(
        encoding="utf-8",
    )

    parsed_content = json.loads(content)

    assert parsed_content["data"] == {
        "test": True,
    }