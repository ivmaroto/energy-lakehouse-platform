"""
Airflow DAG for 15-minute Open-Meteo ingestion.
"""

import json

from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.open_meteo.ingest import OpenMeteoIngestion


def ingest_location(
    *,
    location_id: str,
    latitude: float,
    longitude: float,
    **context,
) -> str:
    """
    Ingest 15-minute Open-Meteo data for one configured location.
    """

    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    request_end = data_interval_end - timedelta(seconds=1)

    ingestion = OpenMeteoIngestion()

    return ingestion.ingest_minutely_15(
        location_id=location_id,
        latitude=latitude,
        longitude=longitude,
        start_datetime=data_interval_start,
        end_datetime=request_end,
    )


with DAG(
    dag_id="open_meteo_15min",
    description="Ingest 15-minute Open-Meteo data into Bronze.",
    schedule="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["open_meteo", "bronze", "15min"],
) as dag:

    locations = json.loads(
        Variable.get(
            "OPEN_METEO_LOCATIONS",
            default_var="[]",
        )
    )

    for location in locations:
        PythonOperator(
            task_id=f"ingest_{location['location_id']}",
            python_callable=ingest_location,
            op_kwargs={
                "location_id": location["location_id"],
                "latitude": location["latitude"],
                "longitude": location["longitude"],
            },
        )