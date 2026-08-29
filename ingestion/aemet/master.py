"""
Read the AEMET station master from Bronze.
"""

from __future__ import annotations

import json
import re
from typing import Any

from minio import Minio

from ingestion.common.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)


STATIONS_PREFIX = "bronze/aemet/stations/"

_COORDINATE_PATTERN = re.compile(
    r"^(\d{2})(\d{2})(\d{2})([NSEW])$"
)


def parse_aemet_coordinate(
    value: str,
    *,
    allowed_hemispheres: set[str],
) -> float:
    value = str(value).strip().upper()

    match = _COORDINATE_PATTERN.fullmatch(
        value
    )

    if match is None:
        raise ValueError(
            f"Invalid AEMET coordinate: {value!r}"
        )

    degrees = int(match.group(1))
    minutes = int(match.group(2))
    seconds = int(match.group(3))
    hemisphere = match.group(4)

    if hemisphere not in allowed_hemispheres:
        raise ValueError(
            f"Invalid hemisphere {hemisphere!r} "
            f"for {value!r}"
        )

    if minutes >= 60 or seconds > 60:
        raise ValueError(
            f"Invalid AEMET DMS coordinate: {value!r}"
        )

    decimal = (
        degrees
        + minutes / 60.0
        + seconds / 3600.0
    )

    if hemisphere in {"S", "W"}:
        decimal = -decimal

    return decimal


def load_aemet_station_locations(
    client: Minio | None = None,
) -> list[dict[str, Any]]:
    """
    Load every station from the latest AEMET
    station inventory persisted in Bronze.
    """

    client = client or Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    objects = list(
        client.list_objects(
            MINIO_BUCKET,
            prefix=STATIONS_PREFIX,
            recursive=True,
        )
    )

    if not objects:
        raise RuntimeError(
            "No AEMET station master exists in Bronze."
        )

    latest = max(
        objects,
        key=lambda obj: obj.last_modified,
    )

    response = client.get_object(
        MINIO_BUCKET,
        latest.object_name,
    )

    try:
        payload = json.loads(
            response.read().decode("utf-8")
        )
    finally:
        response.close()
        response.release_conn()

    stations = payload.get("data")

    if not isinstance(stations, list):
        raise RuntimeError(
            "Unexpected AEMET station master structure."
        )

    locations = []
    station_ids = set()

    for station in stations:
        station_id = str(
            station.get("indicativo", "")
        ).strip()

        if not station_id:
            raise ValueError(
                "AEMET station without indicativo."
            )

        if station_id in station_ids:
            raise ValueError(
                f"Duplicated AEMET station_id: {station_id}"
            )

        station_ids.add(station_id)

        locations.append(
            {
                "station_id": station_id,
                "station_name": station.get("nombre"),
                "province": station.get("provincia"),
                "latitude": parse_aemet_coordinate(
                    station.get("latitud"),
                    allowed_hemispheres={"N", "S"},
                ),
                "longitude": parse_aemet_coordinate(
                    station.get("longitud"),
                    allowed_hemispheres={"E", "W"},
                ),
            }
        )

    return sorted(
        locations,
        key=lambda item: item["station_id"],
    )
