"""
Airflow DAG for daily AEMET ingestion.

Includes:
- AEMET daily climatological values.
- AEMET radiation network data.
"""

from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.aemet.ingest import AemetIngestion


def ingest_aemet_daily_climatology(
    *,
    station_id: str,
    **context,
) -> str:
    """
    Ingest daily climatological values for one configured station.
    """

    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    ingestion = AemetIngestion()

    return ingestion.ingest_incremental(
        station_id=station_id,
        start_date=data_interval_start.date(),
        end_date=(data_interval_end - timedelta(days=1)).date(),
    )


def ingest_aemet_radiation() -> str:
    """
    Ingest the daily AEMET radiation dataset.
    """

    ingestion = AemetIngestion()

    return ingestion.ingest_radiation()


with DAG(
    dag_id="daily_ingestion",
    description="Daily AEMET climatology and radiation ingestion.",
    schedule="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["aemet", "bronze", "daily"],
) as dag:

    stations = Variable.get(
        "AEMET_DAILY_STATIONS",
        default_var=[],
        deserialize_json=True,
    )

    for station in stations:
        PythonOperator(
            task_id=f"climatology_{station['station_id']}",
            python_callable=ingest_aemet_daily_climatology,
            op_kwargs={
                "station_id": station["station_id"],
            },
        )

    PythonOperator(
        task_id="aemet_radiation",
        python_callable=ingest_aemet_radiation,
    )