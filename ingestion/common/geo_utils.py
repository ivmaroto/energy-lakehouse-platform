"""
Geographic utility functions used by the ingestion layer.
"""


def aemet_coordinate_to_decimal(value: str) -> float:
    """
    Convert an AEMET geographic coordinate to decimal degrees.

    AEMET coordinates use degrees, minutes and seconds followed
    by a hemisphere indicator.

    South and West coordinates are returned as negative values.
    """

    value = value.strip().upper()

    if not value:
        raise ValueError("AEMET coordinate cannot be empty.")

    hemisphere = value[-1]
    numeric = value[:-1]

    if hemisphere in ("N", "S", "E", "W"):
        degrees_digits = 2
    else:
        raise ValueError(
            f"Invalid hemisphere in AEMET coordinate: {value}"
        )

    expected_length = degrees_digits + 4

    if len(numeric) != expected_length or not numeric.isdigit():
        raise ValueError(
            f"Invalid AEMET coordinate format: {value}"
        )

    degrees = int(numeric[:degrees_digits])
    minutes = int(
        numeric[degrees_digits:degrees_digits + 2]
    )
    seconds = int(
        numeric[degrees_digits + 2:degrees_digits + 4]
    )

    if minutes >= 60 or seconds > 60:
        raise ValueError(
            f"Invalid AEMET coordinate value: {value}"
        )

    decimal = (
        degrees
        + minutes / 60
        + seconds / 3600
    )

    if hemisphere in ("S", "W"):
        decimal *= -1

    return decimal