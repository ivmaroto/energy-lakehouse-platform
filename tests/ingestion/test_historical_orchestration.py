from datetime import date

import pytest

from ingestion.orchestration import historical_reload


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


def test_run_bronze_historical_reload_coordinates_steps(
    monkeypatch,
):
    calls = []

    monkeypatch.setattr(
        historical_reload,
        "ingest_masters",
        lambda: calls.append("masters") or {
            "aemet_stations": 1,
            "cnig_files": 3,
        },
    )

    monkeypatch.setattr(
        historical_reload,
        "ingest_esios_hourly",
        lambda start, end:
        calls.append(("esios_hourly", start, end)) or 11,
    )

    monkeypatch.setattr(
        historical_reload,
        "ingest_esios_monthly",
        lambda start, end:
        calls.append(("esios_monthly", start, end)) or 9,
    )

    monkeypatch.setattr(
        historical_reload,
        "ingest_open_meteo",
        lambda start, end:
        calls.append(("open_meteo", start, end)) or {
            "locations": 921,
            "hourly_files": 921,
            "minutely_15_files": 921,
        },
    )

    monkeypatch.setattr(
        historical_reload,
        "ingest_aemet_current",
        lambda:
        calls.append("aemet_current") or 1,
    )

    start = date(2026, 8, 24)
    end = date(2026, 8, 29)

    result = (
        historical_reload
        .run_bronze_historical_reload(
            start_date=start,
            end_date=end,
        )
    )

    assert calls == [
        "masters",
        ("esios_hourly", start, end),
        ("esios_monthly", start, end),
        ("open_meteo", start, end),
        "aemet_current",
    ]

    assert result["start_date"] == "2026-08-24"
    assert result["end_date"] == "2026-08-29"
    assert result["esios_hourly_files"] == 11
    assert result["esios_monthly_files"] == 9
    assert result["open_meteo"]["locations"] == 921
