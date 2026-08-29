from datetime import date, datetime, timedelta

import pytest

from ingestion.common.config import OPEN_METEO_ARCHIVE_URL
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


def test_ingest_hourly_range_locations_batches_and_persists():
    http_client = FakeHTTPClient()
    storage = FakeStorage()

    ingestion = OpenMeteoBatchIngestion(
        http_client=http_client,
        storage=storage,
        batch_size=2,
        batch_delay_seconds=0,
    )

    paths = (
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

    assert len(paths) == 3

    # 3 locations with batch_size=2
    # -> 2 HTTP requests.
    assert len(
        http_client.calls
    ) == 2

    assert all(
        call["url"]
        == OPEN_METEO_ARCHIVE_URL
        for call in http_client.calls
    )

    for call in http_client.calls:
        params = call["params"]

        assert (
            params["start_date"]
            == "2026-08-24"
        )

        assert (
            params["end_date"]
            == "2026-08-29"
        )

        assert (
            params["timezone"]
            == "UTC"
        )

        assert "hourly" in params

    assert len(
        storage.calls
    ) == 3

    for storage_call, location in zip(
        storage.calls,
        LOCATIONS,
        strict=True,
    ):
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
                "ingestion_mode"
            ]
            == "historical"
        )

        assert (
            storage_call[
                "requested_start_date"
            ]
            == "2026-08-24"
        )

        assert (
            storage_call[
                "requested_end_date"
            ]
            == "2026-08-29"
        )

        assert (
            storage_call[
                "extra_metadata"
            ]["station_id"]
            == location[
                "station_id"
            ]
        )

        hourly = (
            storage_call["data"][
                "hourly"
            ]
        )

        # 6 complete days x 24 hours.
        assert len(
            hourly["time"]
        ) == 144

        assert (
            hourly["time"][0]
            == "2026-08-24T00:00"
        )

        assert (
            hourly["time"][-1]
            == "2026-08-29T23:00"
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
