import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(
    PROJECT_ROOT / ".env"
)

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"


# ============================================================================
# API credentials
# ============================================================================

AEMET_API_KEY = os.getenv(
    "AEMET_API_KEY"
)

ESIOS_API_KEY = os.getenv(
    "ESIOS_API_KEY"
)


# ============================================================================
# MinIO configuration
# ============================================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "localhost:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ROOT_USER"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_ROOT_PASSWORD"
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "energy-lakehouse",
)

MINIO_SECURE = (
    os.getenv(
        "MINIO_SECURE",
        "false",
    ).lower()
    == "true"
)


# ============================================================================
# API endpoints
# ============================================================================

AEMET_BASE_URL = (
    "https://opendata.aemet.es/"
    "opendata/api"
)

OPEN_METEO_BASE_URL = (
    "https://api.open-meteo.com/"
    "v1/forecast"
)

OPEN_METEO_ARCHIVE_URL = (
    "https://archive-api.open-meteo.com/"
    "v1/archive"
)

OPEN_METEO_HISTORICAL_FORECAST_URL = (
    "https://historical-forecast-api."
    "open-meteo.com/v1/forecast"
)

ESIOS_BASE_URL = (
    "https://api.esios.ree.es"
)


# ============================================================================
# CNIG / IGN configuration
# ============================================================================

CNIG_BASE_URL = (
    "https://centrodedescargas.cnig.es/"
    "CentroDescargas"
)

CNIG_NGMEP_SEQUENTIAL = "9000004"
CNIG_NGMEP_SERIES = "NGMEN"
CNIG_NGMEP_LICENSE = "11"


# ============================================================================
# HTTP configuration
# ============================================================================

HTTP_TIMEOUT = int(
    os.getenv(
        "HTTP_TIMEOUT",
        "30",
    )
)

HTTP_MAX_RETRIES = int(
    os.getenv(
        "HTTP_MAX_RETRIES",
        "3",
    )
)

HTTP_RETRY_BACKOFF_FACTOR = float(
    os.getenv(
        "HTTP_RETRY_BACKOFF_FACTOR",
        "2",
    )
)

HTTP_RETRY_BACKOFF_MAX_SECONDS = int(
    os.getenv(
        "HTTP_RETRY_BACKOFF_MAX_SECONDS",
        "120",
    )
)


# ============================================================================
# Historical ingestion chunk sizes
# ============================================================================

AEMET_HISTORICAL_CHUNK_DAYS = int(
    os.getenv(
        "AEMET_HISTORICAL_CHUNK_DAYS",
        "31",
    )
)

OPEN_METEO_HISTORICAL_CHUNK_DAYS = int(
    os.getenv(
        "OPEN_METEO_HISTORICAL_CHUNK_DAYS",
        "31",
    )
)

ESIOS_HISTORICAL_CHUNK_DAYS = int(
    os.getenv(
        "ESIOS_HISTORICAL_CHUNK_DAYS",
        "31",
    )
)


# ============================================================================
# Open-Meteo resilience
# ============================================================================

OPEN_METEO_MAX_RETRIES = int(
    os.getenv(
        "OPEN_METEO_MAX_RETRIES",
        "8",
    )
)

OPEN_METEO_BATCH_DELAY_SECONDS = float(
    os.getenv(
        "OPEN_METEO_BATCH_DELAY_SECONDS",
        "12",
    )
)