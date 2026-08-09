"""
Custom exceptions used by the ingestion layer.
"""


class IngestionError(Exception):
    """Base exception for ingestion-related errors."""


class ConfigurationError(IngestionError):
    """Raised when required configuration is missing or invalid."""


class APIConnectionError(IngestionError):
    """Raised when a connection to an external API cannot be established."""


class APIRequestError(IngestionError):
    """Raised when an external API returns an unsuccessful response."""


class APIAuthenticationError(APIRequestError):
    """Raised when authentication against an external API fails."""


class APIResponseError(IngestionError):
    """Raised when an API response is invalid or cannot be processed."""


class EmptyResponseError(APIResponseError):
    """Raised when an API returns an unexpected empty response."""


class InvalidDateRangeError(IngestionError):
    """Raised when an ingestion date range is invalid."""


class StorageError(IngestionError):
    """Raised when Bronze data cannot be persisted."""