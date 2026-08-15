import os
from pathlib import Path
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"

# API credentials
AEMET_API_KEY = os.getenv("AEMET_API_KEY")
ESIOS_API_KEY = os.getenv("ESIOS_API_KEY")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "energy-lakehouse")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# API endpoints
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"


OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_HISTORICAL_FORECAST_URL = (
    "https://historical-forecast-api.open-meteo.com/v1/forecast"
)

ESIOS_BASE_URL = "https://api.esios.ree.es"

# HTTP configuration
HTTP_TIMEOUT = 30
HTTP_MAX_RETRIES = 3

# Historical ingestion chunk sizes (days)
AEMET_HISTORICAL_CHUNK_DAYS = 31
OPEN_METEO_HISTORICAL_CHUNK_DAYS = 31
ESIOS_HISTORICAL_CHUNK_DAYS = 31