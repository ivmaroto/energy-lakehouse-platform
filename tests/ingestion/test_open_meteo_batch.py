from ingestion.open_meteo.batch import (
    OpenMeteoBatchIngestion,
)

from datetime import datetime, timezone

from ingestion.common.storage import MinIOBronzeStorage

def test_batch_size():
    ingestion = OpenMeteoBatchIngestion(
        http_client=object(),
        storage=object(),
        batch_size=100,
    )

    locations = [
        {"station_id": str(index)}
        for index in range(921)
    ]

    batches = list(
        ingestion._batches(locations)
    )

    assert len(batches) == 10
    assert len(batches[0]) == 100
    assert len(batches[-1]) == 21


def test_multi_response_count_validation():
    result = (
        OpenMeteoBatchIngestion
        ._normalize_response(
            [{}, {}],
            2,
        )
    )

    assert len(result) == 2


class FakeHourlyHTTPClient:
    def __init__(self):
        self.calls = []

    def get_json(
        self,
        url,
        params=None,
    ):
        self.calls.append(
            {
                "url": url,
                "params": params,
            }
        )

        return [
            {
                "hourly": {
                    "time": [
                        "2026-08-30T11:00"
                    ],
                    "temperature_2m": [
                        99.0
                    ],
                }
            }
        ]


class FakeMinIOHourlyStorage(
    MinIOBronzeStorage
):
    def __init__(
        self,
        existing_payload,
    ):
        self.existing_payload = (
            existing_payload
        )

        self.calls = []

    def object_exists(
        self,
        object_name,
    ):
        return True

    def read_json(
        self,
        object_name,
    ):
        return self.existing_payload

    def save_json(
        self,
        data,
        **kwargs,
    ):
        self.calls.append(
            {
                "data": data,
                **kwargs,
            }
        )

        return kwargs[
            "object_name"
        ]


def test_ingest_hourly_locations_merges_existing_daily_object():
    existing_payload = {
        "metadata": {},
        "data": {
            "hourly": {
                "time": [
                    "2026-08-30T10:00",
                    "2026-08-30T11:00",
                ],
                "temperature_2m": [
                    20.0,
                    21.0,
                ],
            }
        },
    }

    http_client = (
        FakeHourlyHTTPClient()
    )

    storage = (
        FakeMinIOHourlyStorage(
            existing_payload
        )
    )

    ingestion = (
        OpenMeteoBatchIngestion(
            http_client=http_client,
            storage=storage,
            batch_size=100,
            batch_delay_seconds=0,
        )
    )

    location = {
        "station_id": "TEST",
        "station_name": (
            "Test Station"
        ),
        "province": "Madrid",
        "latitude": 40.0,
        "longitude": -3.0,
    }

    paths = (
        ingestion
        .ingest_hourly_locations(
            locations=[
                location
            ],
            target_hour=datetime(
                2026,
                8,
                30,
                11,
                0,
                tzinfo=timezone.utc,
            ),
            resume=False,
        )
    )

    assert len(
        http_client.calls
    ) == 1

    assert len(
        storage.calls
    ) == 1

    saved = (
        storage.calls[0]
    )

    assert (
        saved["object_name"]
        ==
        "bronze/open_meteo/"
        "weather_hourly/"
        "year=2026/month=08/day=30/"
        "station_id=TEST.json"
    )

    hourly = (
        saved[
            "data"
        ][
            "hourly"
        ]
    )

    assert hourly[
        "time"
    ] == [
        "2026-08-30T10:00",
        "2026-08-30T11:00",
    ]

    assert hourly[
        "temperature_2m"
    ] == [
        20.0,
        99.0,
    ]

    assert (
        len(
            hourly["time"]
        )
        ==
        len(
            set(
                hourly["time"]
            )
        )
    )

    assert paths == [
        (
            "bronze/open_meteo/"
            "weather_hourly/"
            "year=2026/month=08/day=30/"
            "station_id=TEST.json"
        )
    ]