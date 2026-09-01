from datetime import date

import pytest

from ingestion.orchestration import historical_reload


# ============================================================================
# Date validation
# ============================================================================

def test_validate_date_range_accepts_valid_range():
    historical_reload.validate_date_range(
        date(2026, 8, 24),
        date(2026, 8, 29),
    )


def test_validate_date_range_rejects_invalid_range():
    with pytest.raises(
        ValueError,
        match="start_date cannot be after end_date",
    ):
        historical_reload.validate_date_range(
            date(2026, 8, 29),
            date(2026, 8, 24),
        )


def test_get_monthly_range_expands_to_complete_month():
    start, end = historical_reload.get_monthly_range(
        date(2026, 8, 24),
        date(2026, 8, 29),
    )

    assert start == date(2026, 8, 1)
    assert end == date(2026, 8, 31)


def test_iter_dates_is_inclusive():
    result = list(
        historical_reload.iter_dates(
            date(2026, 7, 10),
            date(2026, 7, 12),
        )
    )

    assert result == [
        date(2026, 7, 10),
        date(2026, 7, 11),
        date(2026, 7, 12),
    ]


def test_iter_months_returns_touched_months():
    result = list(
        historical_reload.iter_months(
            date(2026, 7, 28),
            date(2026, 9, 2),
        )
    )

    assert result == [
        date(2026, 7, 1),
        date(2026, 8, 1),
        date(2026, 9, 1),
    ]


# ============================================================================
# Bronze range deletion
# ============================================================================

def test_delete_bronze_range_deletes_only_historical_fact_partitions(
    monkeypatch,
):
    deleted_prefixes = []

    class FakeStorage:
        def delete_prefix(
            self,
            prefix,
        ):
            deleted_prefixes.append(
                prefix
            )

            return 1

    monkeypatch.setattr(
        historical_reload,
        "MinIOBronzeStorage",
        FakeStorage,
    )

    def fake_load_esios_indicators(
        grain,
    ):
        if grain == "hourly":
            return {
                1: "hourly_a",
                2: "hourly_b",
            }

        if grain == "monthly":
            return {
                3: "monthly_a",
                4: "monthly_b",
            }

        raise AssertionError(
            f"Unexpected grain: {grain}"
        )

    monkeypatch.setattr(
        historical_reload,
        "load_esios_indicators",
        fake_load_esios_indicators,
    )

    result = (
        historical_reload
        .delete_bronze_range(
            date(2026, 7, 10),
            date(2026, 7, 11),
        )
    )

    assert result == {
        "open_meteo_hourly": 2,
        "open_meteo_15min": 2,
        "esios_hourly": 4,
        "esios_monthly": 2,
    }

    expected_prefixes = {
        # Open-Meteo hourly
        (
            "bronze/open_meteo/weather_hourly/"
            "year=2026/month=07/day=10/"
        ),
        (
            "bronze/open_meteo/weather_hourly/"
            "year=2026/month=07/day=11/"
        ),

        # Open-Meteo 15-minute
        (
            "bronze/open_meteo/weather_15min/"
            "year=2026/month=07/day=10/"
        ),
        (
            "bronze/open_meteo/weather_15min/"
            "year=2026/month=07/day=11/"
        ),

        # ESIOS hourly
        (
            "bronze/esios/hourly_a/"
            "year=2026/month=07/day=10/"
        ),
        (
            "bronze/esios/hourly_a/"
            "year=2026/month=07/day=11/"
        ),
        (
            "bronze/esios/hourly_b/"
            "year=2026/month=07/day=10/"
        ),
        (
            "bronze/esios/hourly_b/"
            "year=2026/month=07/day=11/"
        ),

        # ESIOS monthly
        (
            "bronze/esios/monthly_a/"
            "year=2026/month=07/"
        ),
        (
            "bronze/esios/monthly_b/"
            "year=2026/month=07/"
        ),
    }

    assert set(
        deleted_prefixes
    ) == expected_prefixes

    # Masters must never be deleted by range overwrite.
    assert not any(
        prefix.startswith(
            "bronze/aemet/stations/"
        )
        for prefix in deleted_prefixes
    )

    assert not any(
        prefix.startswith(
            "bronze/cnig/"
        )
        for prefix in deleted_prefixes
    )


def test_delete_bronze_range_deletes_all_touched_months(
    monkeypatch,
):
    deleted_prefixes = []

    class FakeStorage:
        def delete_prefix(
            self,
            prefix,
        ):
            deleted_prefixes.append(
                prefix
            )

            return 1

    monkeypatch.setattr(
        historical_reload,
        "MinIOBronzeStorage",
        FakeStorage,
    )

    def fake_load_esios_indicators(
        grain,
    ):
        if grain == "hourly":
            return {}

        if grain == "monthly":
            return {
                1: "monthly_test",
            }

        raise AssertionError(
            f"Unexpected grain: {grain}"
        )

    monkeypatch.setattr(
        historical_reload,
        "load_esios_indicators",
        fake_load_esios_indicators,
    )

    historical_reload.delete_bronze_range(
        date(2026, 7, 31),
        date(2026, 8, 1),
    )

    assert (
        "bronze/esios/monthly_test/"
        "year=2026/month=07/"
    ) in deleted_prefixes

    assert (
        "bronze/esios/monthly_test/"
        "year=2026/month=08/"
    ) in deleted_prefixes


# ============================================================================
# Complete Bronze deletion
# ============================================================================

def test_delete_all_bronze_uses_active_bronze_prefix(
    monkeypatch,
):
    calls = []

    class FakeStorage:
        def delete_prefix(
            self,
            prefix,
        ):
            calls.append(
                prefix
            )

            return 123

    monkeypatch.setattr(
        historical_reload,
        "MinIOBronzeStorage",
        FakeStorage,
    )

    result = (
        historical_reload
        .delete_all_bronze()
    )

    assert result == 123

    assert calls == [
        "bronze/",
    ]


# ============================================================================
# Complete historical Bronze orchestration
# ============================================================================

def test_run_bronze_historical_reload_coordinates_steps(
    monkeypatch,
):
    calls = []

    monkeypatch.setattr(
        historical_reload,
        "ingest_masters",
        lambda:
        calls.append(
            "masters"
        )
        or {
            "aemet_stations": 1,
            "cnig_files": 2,
        },
    )

    monkeypatch.setattr(
        historical_reload,
        "ingest_esios_hourly",
        lambda start, end:
        calls.append(
            (
                "esios_hourly",
                start,
                end,
            )
        )
        or 11,
    )

    monkeypatch.setattr(
        historical_reload,
        "ingest_esios_monthly",
        lambda start, end:
        calls.append(
            (
                "esios_monthly",
                start,
                end,
            )
        )
        or 9,
    )

    monkeypatch.setattr(
        historical_reload,
        "ingest_open_meteo",
        lambda start, end:
        calls.append(
            (
                "open_meteo",
                start,
                end,
            )
        )
        or {
            "locations": 926,
            "hourly_files": 926,
            "minutely_15_files": 926,
        },
    )

    # Historical reload must never ingest AEMET current observations.
    monkeypatch.setattr(
        historical_reload,
        "ingest_aemet_current",
        lambda: (
            pytest.fail(
                "Historical reload must not ingest AEMET current."
            )
        ),
    )

    start = date(
        2026,
        8,
        24,
    )

    end = date(
        2026,
        8,
        29,
    )

    result = (
        historical_reload
        .run_bronze_historical_reload(
            start_date=start,
            end_date=end,
        )
    )

    assert calls == [
        "masters",
        (
            "esios_hourly",
            start,
            end,
        ),
        (
            "esios_monthly",
            start,
            end,
        ),
        (
            "open_meteo",
            start,
            end,
        ),
    ]

    assert result == {
        "start_date": "2026-08-24",
        "end_date": "2026-08-29",
        "masters": {
            "aemet_stations": 1,
            "cnig_files": 2,
        },
        "esios_hourly_files": 11,
        "esios_monthly_files": 9,
        "open_meteo": {
            "locations": 926,
            "hourly_files": 926,
            "minutely_15_files": 926,
        },
    }

    assert (
        "aemet_current_files"
        not in result
    )