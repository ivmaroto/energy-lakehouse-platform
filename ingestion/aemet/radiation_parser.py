"""
Parser for AEMET special radiation network data.
"""

from __future__ import annotations

import csv
import io
from datetime import date, datetime
from typing import Any


RADIATION_UNITS = {
    "GL": "10*kJ/m2",
    "DF": "10*kJ/m2",
    "DT": "10*kJ/m2",
    "UVB": "J/m2",
    "UVER": "J/m2",
    "IR": "10*kJ/m2",
}


def _parse_date(raw_value: str) -> date:
    """
    Parse the date contained in the AEMET radiation dataset.

    Example:
        13-08-26 -> 2026-08-13
    """

    value = raw_value.strip().strip('"')

    return datetime.strptime(
        value,
        "%d-%m-%y",
    ).date()


def _parse_float(raw_value: str) -> float | None:
    """
    Convert a radiation value to float.

    Empty values are returned as None.
    """

    value = raw_value.strip()

    if not value:
        return None

    try:
        return float(value)
    except ValueError:
        return None


def _normalize_solar_time(raw_value: str) -> str:
    """
    Normalize the AEMET true solar time column.

    Examples:
        5   -> 05:00
        5.5 -> 05:30
        20  -> 20:00
    """

    value = float(raw_value)

    hour = int(value)

    minutes = 30 if value % 1 else 0

    return f"{hour:02d}:{minutes:02d}"


def parse_radiation_data(
    raw_text: str,
) -> list[dict[str, Any]]:
    """
    Parse AEMET special radiation network data into normalized records.

    The AEMET source contains one row per station and multiple radiation
    blocks in the same row:

        GL  -> Global radiation, hourly
        DF  -> Diffuse radiation, hourly
        DT  -> Direct radiation, hourly
        UVB -> Erythemal ultraviolet radiation, half-hourly
        IR  -> Infrared radiation, hourly

    A normalized record is generated for every temporal observation.

    Returned fields:
        station_name
        station_id
        observation_date
        radiation_type
        solar_time
        value
        unit
        daily_total
        temporal_granularity
    """

    if not raw_text or not raw_text.strip():
        raise ValueError(
            "AEMET radiation dataset is empty."
        )

    rows = list(
        csv.reader(
            io.StringIO(raw_text),
            delimiter=";",
            quotechar='"',
        )
    )

    if len(rows) < 4:
        raise ValueError(
            "Unexpected AEMET radiation dataset structure."
        )

    # Row 0: "RADIACION SOLAR"
    # Row 1: date, e.g. "13-08-26"
    # Row 2: header
    # Row 3+: station data

    observation_date = _parse_date(rows[1][0])

    header = rows[2]

    if len(header) < 3:
        raise ValueError(
            "Invalid AEMET radiation header."
        )

    records: list[dict[str, Any]] = []

    for row in rows[3:]:
        if len(row) < 3:
            continue

        station_name = row[0].strip()
        station_id = row[1].strip()

        if not station_name or not station_id:
            continue

        column_index = 2

        while column_index < len(row):
            radiation_type = row[column_index].strip()

            if radiation_type not in RADIATION_UNITS:
                column_index += 1
                continue

            unit = RADIATION_UNITS[radiation_type]

            if radiation_type in {"GL", "DF", "DT"}:
                time_values = [
                    str(hour)
                    for hour in range(5, 21)
                ]

                temporal_granularity = "1h"

            elif radiation_type in {"UVB", "UVER"}:
                time_values = [
                    str(hour / 2)
                    for hour in range(9, 41)
                ]

                temporal_granularity = "30min"

            elif radiation_type == "IR":
                time_values = [
                    str(hour)
                    for hour in range(1, 25)
                ]

                temporal_granularity = "1h"

            else:
                column_index += 1
                continue

            first_value_index = column_index + 1
            last_value_index = (
                first_value_index + len(time_values)
            )

            if last_value_index >= len(row):
                break

            values = row[
                first_value_index:last_value_index
            ]

            daily_total = _parse_float(
                row[last_value_index]
            )

            for solar_time, raw_value in zip(
                time_values,
                values,
            ):
                value = _parse_float(raw_value)

                records.append(
                    {
                        "station_name": station_name,
                        "station_id": station_id,
                        "observation_date": (
                            observation_date.isoformat()
                        ),
                        "radiation_type": radiation_type,
                        "solar_time": _normalize_solar_time(
                            solar_time
                        ),
                        "value": value,
                        "unit": unit,
                        "daily_total": daily_total,
                        "temporal_granularity": (
                            temporal_granularity
                        ),
                    }
                )

            # Skip:
            # - radiation type column
            # - temporal values
            # - SUMA column
            column_index = last_value_index + 1

    return records