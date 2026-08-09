"""
Common logging configuration for the ingestion layer.
"""

import logging
import sys


DEFAULT_LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Create or retrieve a logger using the common ingestion format.

    Parameters
    ----------
    name:
        Name of the logger, normally __name__.
    level:
        Logging level. Defaults to logging.INFO.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers when the logger is requested
    # multiple times during the same execution.
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            fmt=DEFAULT_LOG_FORMAT,
            datefmt=DEFAULT_DATE_FORMAT,
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False

    return logger