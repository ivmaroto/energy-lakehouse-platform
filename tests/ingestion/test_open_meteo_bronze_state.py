import json

from types import SimpleNamespace

import pytest

from ingestion.common.exceptions import StorageError
from ingestion.open_meteo.bronze_state import (
    find_completed_location_ids,
)


class FakeResponse:
    def __init__(
        self,
        data: bytes,
    ):
        self.data = data

    def read(self):
        return self.data

    def close(self):
        pass

    def release_conn(self):
        pass


class FakeMinioClient:
    def __init__(
        self,
        objects,
    ):
        self.objects = objects

    def list_objects(
        self,
        bucket,
        prefix,
        recursive,
    ):
        return [
            SimpleNamespace(
                object_name=name
            )
            for name in self.objects
            if name.startswith(prefix)
        ]

    def get_object(
        self,
        bucket,
        object_name,
    ):
        return FakeResponse(
            self.objects[
                object_name
            ]
        )


class FakeStorage:
    def __init__(
        self,
        objects,
    ):
        self.bucket = "test-bucket"

        self.client = (
            FakeMinioClient(
                objects
            )
        )


def build_hourly_axis():
    axis = []

    for day in range(
        24,
        30,
    ):
        for hour in range(
            24
        ):
            axis.append(
                (
                    f"2026-08-{day:02d}"
                    f"T{hour:02d}:00"
                )
            )

    return axis


def build_payload(
    *,
    station_id,
    start_date=(
        "2026-08-24"
    ),
    end_date=(
        "2026-08-29"
    ),
    axis=None,
):
    if axis is None:
        axis = build_hourly_axis()

    return {
        "metadata": {
            "source": (
                "open_meteo"
            ),
            "dataset": (
                "weather_hourly"
            ),
            "ingestion_mode": (
                "historical"
            ),
            "requested_start_date": (
                start_date
            ),
            "requested_end_date": (
                end_date
            ),
            "station_id": (
                station_id
            ),
        },
        "data": {
            "hourly": {
                "time": axis,
                "temperature_2m": [
                    20.0
                    for _ in axis
                ],
            }
        },
    }


def encode_payload(
    payload,
):
    return json.dumps(
        payload
    ).encode(
        "utf-8"
    )


def find_completed(
    storage,
):
    return (
        find_completed_location_ids(
            storage=storage,
            source="open_meteo",
            dataset=(
                "weather_hourly"
            ),
            requested_start_date=(
                "2026-08-24"
            ),
            requested_end_date=(
                "2026-08-29"
            ),
            ingestion_mode=(
                "historical"
            ),
            id_fields=(
                "station_id",
            ),
        )
    )


def test_complete_hourly_object_is_reused():
    object_name = (
        "bronze/open_meteo/"
        "weather_hourly/"
        "complete.json"
    )

    storage = FakeStorage(
        {
            object_name: (
                encode_payload(
                    build_payload(
                        station_id="A"
                    )
                )
            )
        }
    )

    completed = (
        find_completed(
            storage
        )
    )

    assert completed == {
        "A"
    }


def test_incomplete_hourly_object_is_not_reused():
    axis = build_hourly_axis()[
        :-1
    ]

    assert len(axis) == 143

    object_name = (
        "bronze/open_meteo/"
        "weather_hourly/"
        "incomplete.json"
    )

    storage = FakeStorage(
        {
            object_name: (
                encode_payload(
                    build_payload(
                        station_id="A",
                        axis=axis,
                    )
                )
            )
        }
    )

    completed = (
        find_completed(
            storage
        )
    )

    assert completed == set()


def test_wrong_interval_is_not_reused():
    object_name = (
        "bronze/open_meteo/"
        "weather_hourly/"
        "wrong-range.json"
    )

    storage = FakeStorage(
        {
            object_name: (
                encode_payload(
                    build_payload(
                        station_id="A",
                        start_date=(
                            "2026-08-23"
                        ),
                    )
                )
            )
        }
    )

    completed = (
        find_completed(
            storage
        )
    )

    assert completed == set()


def test_invalid_json_fails_closed():
    object_name = (
        "bronze/open_meteo/"
        "weather_hourly/"
        "corrupt.json"
    )

    storage = FakeStorage(
        {
            object_name: (
                b"{invalid-json"
            )
        }
    )

    with pytest.raises(
        StorageError,
        match=(
            "Invalid Bronze JSON"
        ),
    ):
        find_completed(
            storage
        )