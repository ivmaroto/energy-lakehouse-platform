import os

SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "change-this-secret-key-in-production",
)

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.environ.get('POSTGRES_USER')}:"
    f"{os.environ.get('POSTGRES_PASSWORD')}"
    "@postgres:5432/superset"
)

WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False