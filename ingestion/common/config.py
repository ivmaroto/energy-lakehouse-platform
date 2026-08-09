import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"

# API credentials
AEMET_API_KEY = os.getenv("AEMET_API_KEY")
ESIOS_API_KEY = os.getenv("ESIOS_API_KEY")

# API endpoints
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

ESIOS_BASE_URL = "https://api.esios.ree.es"

# HTTP configuration
HTTP_TIMEOUT = 30
HTTP_MAX_RETRIES = 3