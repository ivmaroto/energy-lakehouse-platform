from datetime import date, datetime, timedelta

from ingestion.common.storage import MinIOBronzeStorage

import pytest

from ingestion.common.config import (
    OPEN_METEO_ARCHIVE_URL,
    OPEN_METEO_HISTORICAL_FORECAST_URL,
)
from ingestion.open_meteo.batch import OpenMeteoBatchIngestion


def build_hourly_axis(
    start_date: str,
    end_date: str,
) -> list[str]:
    """
    Build the complete inclusive hourly UTC axis expected
    from Open-Meteo for a historical date interval.
    """

    current = datetime.fromisoformat(
        f"{start_date}T00:00"
    )

    end = datetime.fromisoformat(
        f"{end_date}T23:00"
    )

    axis = []

    while current <= end:
        axis.append(
            current.strftime(
                "%Y-%m-%dT%H:%M"
            )
        )

        current += timedelta(
            hours=1
        )

    return axis


class FakeHTTPClient:
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

        latitudes = (
            params["latitude"]
            .split(",")
        )

        hourly_axis = build_hourly_axis(
            params["start_date"],
            params["end_date"],
        )

        return [
            {
                "latitude": float(
                    latitude
                ),
                "hourly": {
                    "time": hourly_axis,
                    "temperature_2m": [
                        20.0
                        for _ in hourly_axis
                    ],
                },
            }
            for latitude in latitudes
        ]


class IncompleteFakeHTTPClient:
    """
    Simulate an HTTP 200 response whose temporal
    coverage is incomplete.
    """

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

        latitudes = (
            params["latitude"]
            .split(",")
        )

        return [
            {
                "latitude": float(
                    latitude
                ),
                "hourly": {
                    "time": [
                        "2026-08-24T00:00"
                    ],
                    "temperature_2m": [
                        20.0
                    ],
                },
            }
            for latitude in latitudes
        ]


class FakeStorage:
    def __init__(self):
        self.calls = []

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

        return (
            f"bronze-"
            f"{len(self.calls)}.json"
        )

class FakeMinIOStorage(MinIOBronzeStorage):
    def __init__(self, existing=None):
        self.existing = existing or {}
        self.calls = []

    def object_exists(
        self,
        object_name,
    ):
        return (
            object_name
            in self.existing
        )

    def read_json(
        self,
        object_name,
    ):
        return self.existing[
            object_name
        ]

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


LOCATIONS = [
    {
        "station_id": "A",
        "station_name": "Station A",
        "province": "Madrid",
        "latitude": 40.1,
        "longitude": -3.1,
    },
    {
        "station_id": "B",
        "station_name": "Station B",
        "province": "Madrid",
        "latitude": 40.2,
        "longitude": -3.2,
    },
    {
        "station_id": "C",
        "station_name": "Station C",
        "province": "Toledo",
        "latitude": 39.9,
        "longitude": -4.0,
    },
]


def build_15min_axis(
    day: str,
) -> list[str]:
    current = datetime.fromisoformat(
        f"{day}T00:00"
    )

    end = datetime.fromisoformat(
        f"{day}T23:45"
    )

    axis = []

    while current <= end:
        axis.append(
            current.strftime(
                "%Y-%m-%dT%H:%M"
            )
        )

        current += timedelta(
            minutes=15
        )

    return axis


class Fake15MinHTTPClient:
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

        latitudes = (
            params["latitude"]
            .split(",")
        )

        requested_day = (
            params[
                "start_minutely_15"
            ][:10]
        )

        axis = build_15min_axis(
            requested_day
        )

        return [
            {
                "latitude": float(
                    latitude
                ),
                "minutely_15": {
                    "time": axis,
                    "temperature_2m": [
                        20.0
                        for _ in axis
                    ],
                },
            }
            for latitude in latitudes
        ]


def test_ingest_hourly_range_locations_batches_and_persists():
    http_client = FakeHTTPClient()
    storage = FakeStorage()

    ingestion = OpenMeteoBatchIngestion(
        http_client=http_client,
        storage=storage,
        batch_size=2,
        batch_delay_seconds=0,
    )

    start_date = date(
        2026,
        8,
        24,
    )

    end_date = date(
        2026,
        8,
        29,
    )

    paths = (
        ingestion
        .ingest_hourly_range_locations(
            locations=LOCATIONS,
            start_date=start_date,
            end_date=end_date,
        )
    )

    expected_days = [
        start_date
        + timedelta(days=offset)
        for offset in range(6)
    ]

    # 6 days × 3 stations.
    assert len(paths) == 18

    # 3 locations with batch_size=2:
    # 2 HTTP requests per day × 6 days.
    assert len(
        http_client.calls
    ) == 12

    for day_index, expected_day in enumerate(
        expected_days
    ):
        day_text = (
            expected_day.isoformat()
        )

        day_calls = (
            http_client.calls[
                day_index * 2:
                (day_index + 1) * 2
            ]
        )

        assert len(
            day_calls
        ) == 2

        for call in day_calls:
            assert (
                call["url"]
                == OPEN_METEO_ARCHIVE_URL
            )

            params = (
                call["params"]
            )

            assert (
                params["start_date"]
                == day_text
            )

            assert (
                params["end_date"]
                == day_text
            )

            assert (
                params["timezone"]
                == "UTC"
            )

            assert "hourly" in params

    assert len(
        storage.calls
    ) == 18

    expected_storage_items = [
        (
            expected_day,
            location,
        )
        for expected_day in expected_days
        for location in LOCATIONS
    ]

    for (
        storage_call,
        (
            expected_day,
            location,
        ),
    ) in zip(
        storage.calls,
        expected_storage_items,
        strict=True,
    ):
        day_text = (
            expected_day.isoformat()
        )

        year = (
            expected_day.strftime(
                "%Y"
            )
        )

        month = (
            expected_day.strftime(
                "%m"
            )
        )

        day = (
            expected_day.strftime(
                "%d"
            )
        )

        station_id = str(
            location[
                "station_id"
            ]
        )

        expected_object_name = (
            "bronze/open_meteo/"
            "weather_hourly/"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"station_id={station_id}.json"
        )

        assert (
            storage_call["source"]
            == "open_meteo"
        )

        assert (
            storage_call["dataset"]
            == "weather_hourly"
        )

        assert (
            storage_call[
                "object_name"
            ]
            == expected_object_name
        )

        assert (
            storage_call[
                "ingestion_mode"
            ]
            == "historical"
        )

        assert (
            storage_call[
                "requested_start_date"
            ]
            == day_text
        )

        assert (
            storage_call[
                "requested_end_date"
            ]
            == day_text
        )

        assert (
            storage_call[
                "extra_metadata"
            ][
                "station_id"
            ]
            == location[
                "station_id"
            ]
        )

        assert (
            storage_call[
                "extra_metadata"
            ][
                "observation_date"
            ]
            == day_text
        )

        hourly = (
            storage_call[
                "data"
            ][
                "hourly"
            ]
        )

        assert len(
            hourly["time"]
        ) == 24

        assert (
            hourly["time"][0]
            == f"{day_text}T00:00"
        )

        assert (
            hourly["time"][-1]
            == f"{day_text}T23:00"
        )


def test_ingest_hourly_range_locations_rejects_incomplete_time_axis():
    http_client = (
        IncompleteFakeHTTPClient()
    )

    storage = FakeStorage()

    ingestion = OpenMeteoBatchIngestion(
        http_client=http_client,
        storage=storage,
        batch_size=2,
        batch_delay_seconds=0,
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "temporal coverage "
            "mismatch"
        ),
    ):
        (
            ingestion
            .ingest_hourly_range_locations(
                locations=LOCATIONS,
                start_date=date(
                    2026,
                    8,
                    24,
                ),
                end_date=date(
                    2026,
                    8,
                    29,
                ),
            )
        )

    # Invalid/incomplete responses must
    # never be persisted to Bronze.
    assert storage.calls == []


def test_ingest_hourly_range_locations_rejects_empty_locations():
    ingestion = OpenMeteoBatchIngestion(
        http_client=(
            FakeHTTPClient()
        ),
        storage=FakeStorage(),
        batch_delay_seconds=0,
    )

    with pytest.raises(
        ValueError,
        match=(
            "No Open-Meteo "
            "locations provided"
        ),
    ):
        (
            ingestion
            .ingest_hourly_range_locations(
                locations=[],
                start_date=date(
                    2026,
                    8,
                    24,
                ),
                end_date=date(
                    2026,
                    8,
                    29,
                ),
            )
        )


def test_ingest_hourly_range_locations_rejects_invalid_range():
    ingestion = OpenMeteoBatchIngestion(
        http_client=(
            FakeHTTPClient()
        ),
        storage=FakeStorage(),
        batch_delay_seconds=0,
    )

    with pytest.raises(
        ValueError,
        match=(
            "start_date is after "
            "end_date"
        ),
    ):
        (
            ingestion
            .ingest_hourly_range_locations(
                locations=LOCATIONS,
                start_date=date(
                    2026,
                    8,
                    29,
                ),
                end_date=date(
                    2026,
                    8,
                    24,
                ),
            )
        )


def test_ingest_15min_locations_persists_daily_canonical_objects():
    http_client = Fake15MinHTTPClient()
    storage = FakeStorage()

    ingestion = OpenMeteoBatchIngestion(
        http_client=http_client,
        storage=storage,
        batch_size=2,
        batch_delay_seconds=0,
    )

    start_datetime = datetime(
        2026,
        8,
        24,
        0,
        0,
    )

    end_datetime = datetime(
        2026,
        8,
        25,
        23,
        59,
    )

    paths = (
        ingestion
        .ingest_15min_locations(
            locations=LOCATIONS,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            ingestion_mode="historical",
        )
    )

    # 2 days × 3 stations.
    assert len(paths) == 6
    assert len(storage.calls) == 6

    # batch_size=2:
    # 2 requests/day × 2 days.
    assert len(
        http_client.calls
    ) == 4

    expected_days = [
        "2026-08-24",
        "2026-08-25",
    ]

    for day_index, day_text in enumerate(
        expected_days
    ):
        day_calls = (
            http_client.calls[
                day_index * 2:
                (day_index + 1) * 2
            ]
        )

        assert len(day_calls) == 2

        for call in day_calls:
            assert (
                call["url"]
                == OPEN_METEO_HISTORICAL_FORECAST_URL
            )

            params = call["params"]

            assert (
                params["start_minutely_15"]
                == f"{day_text}T00:00"
            )

            assert (
                params["end_minutely_15"]
                == f"{day_text}T23:59"
            )

            assert (
                params["timezone"]
                == "UTC"
            )

    expected_items = [
        (day_text, location)
        for day_text in expected_days
        for location in LOCATIONS
    ]

    for (
        storage_call,
        (
            day_text,
            location,
        ),
    ) in zip(
        storage.calls,
        expected_items,
        strict=True,
    ):
        year, month, day = (
            day_text.split("-")
        )

        station_id = str(
            location["station_id"]
        )

        expected_object_name = (
            "bronze/open_meteo/"
            "weather_15min/"
            f"year={year}/"
            f"month={month}/"
            f"day={day}/"
            f"station_id={station_id}.json"
        )

        assert (
            storage_call["object_name"]
            == expected_object_name
        )

        assert (
            storage_call["dataset"]
            == "weather_15min"
        )

        assert (
            storage_call[
                "ingestion_mode"
            ]
            == "historical"
        )

        assert (
            storage_call[
                "requested_start_date"
            ]
            == day_text
        )

        assert (
            storage_call[
                "requested_end_date"
            ]
            == day_text
        )

        assert (
            storage_call[
                "extra_metadata"
            ]["station_id"]
            == location["station_id"]
        )

        assert (
            storage_call[
                "extra_metadata"
            ]["observation_date"]
            == day_text
        )

        axis = (
            storage_call[
                "data"
            ][
                "minutely_15"
            ][
                "time"
            ]
        )

        assert len(axis) == 96

        assert (
            axis[0]
            == f"{day_text}T00:00"
        )

        assert (
            axis[-1]
            == f"{day_text}T23:45"
        )


def test_ingest_15min_locations_resume_downloads_only_missing_day():
    location = LOCATIONS[0]

    existing_day = "2026-08-24"

    existing_object = (
        "bronze/open_meteo/"
        "weather_15min/"
        "year=2026/month=08/day=24/"
        "station_id=A.json"
    )

    existing_payload = {
        "metadata": {
            "source": "open_meteo",
            "dataset": "weather_15min",
            "station_id": "A",
        },
        "data": {
            "minutely_15": {
                "time": build_15min_axis(
                    existing_day
                ),
                "temperature_2m": [
                    20.0
                    for _ in range(96)
                ],
            }
        },
    }

    storage = FakeMinIOStorage(
        existing={
            existing_object: (
                existing_payload
            )
        }
    )

    http_client = (
        Fake15MinHTTPClient()
    )

    ingestion = OpenMeteoBatchIngestion(
        http_client=http_client,
        storage=storage,
        batch_size=100,
        batch_delay_seconds=0,
    )

    paths = (
        ingestion
        .ingest_15min_locations(
            locations=[
                location
            ],
            start_datetime=datetime(
                2026,
                8,
                24,
                0,
                0,
            ),
            end_datetime=datetime(
                2026,
                8,
                25,
                23,
                59,
            ),
            resume=True,
            ingestion_mode="historical",
        )
    )

    assert len(
        http_client.calls
    ) == 1

    params = (
        http_client.calls[0][
            "params"
        ]
    )

    assert (
        params[
            "start_minutely_15"
        ]
        == "2026-08-25T00:00"
    )

    assert (
        params[
            "end_minutely_15"
        ]
        == "2026-08-25T23:59"
    )

    assert len(
        storage.calls
    ) == 1

    expected_new_object = (
        "bronze/open_meteo/"
        "weather_15min/"
        "year=2026/month=08/day=25/"
        "station_id=A.json"
    )

    assert (
        storage.calls[0][
            "object_name"
        ]
        == expected_new_object
    )

    assert paths == [
        expected_new_object
    ]