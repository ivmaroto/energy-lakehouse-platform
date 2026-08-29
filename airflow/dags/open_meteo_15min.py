"""
15-minute Open-Meteo Bronze ingestion.

The complete AEMET station master is loaded
at task execution time.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.aemet.master import load_aemet_station_locations
from ingestion.open_meteo.batch import OpenMeteoBatchIngestion


def ingest_all_aemet_locations(
    **context,
):
    locations = load_aemet_station_locations()

    start = context["data_interval_start"]
    end = (
        context["data_interval_end"]
        - timedelta(seconds=1)
    )

    paths = (
        OpenMeteoBatchIngestion()
        .ingest_15min_locations(
            locations=locations,
            start_datetime=start,
            end_datetime=end,
        )
    )

    return (
        f"{len(locations)} stations; "
        f"{len(paths)} Bronze files"
    )


with DAG(
    dag_id="open_meteo_15min",
    description=(
        "15-minute Open-Meteo ingestion for "
        "the complete AEMET station master."
    ),
    schedule="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=[
        "open_meteo",
        "aemet_master",
        "bronze",
        "15min",
    ],
) as dag:

    PythonOperator(
        task_id="open_meteo_15min_all_stations",
        python_callable=ingest_all_aemet_locations,
    )
