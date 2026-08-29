"""
Hourly Bronze ingestion.

Includes:
- Approved hourly ESIOS generation indicators.
- AEMET conventional observations.
- Open-Meteo hourly weather for the complete AEMET station master.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.aemet.ingest import AemetIngestion
from ingestion.aemet.master import load_aemet_station_locations
from ingestion.common.esios_config import load_esios_indicators
from ingestion.esios.ingest import EsiosIngestion
from ingestion.open_meteo.batch import OpenMeteoBatchIngestion


ESIOS_HOURLY_INDICATORS = load_esios_indicators("hourly")


def ingest_esios_hourly_indicator(
    *,
    indicator_id: int,
    dataset: str,
    **context,
):
    start = context["data_interval_start"]
    end = (
        context["data_interval_end"]
        - timedelta(seconds=1)
    )

    return EsiosIngestion().ingest_incremental(
        indicator_id=indicator_id,
        dataset=dataset,
        start_date=start,
        end_date=end,
    )


def ingest_aemet_observations():
    return AemetIngestion().ingest_current_observations()


def ingest_open_meteo_hourly(
    **context,
):
    locations = load_aemet_station_locations()

    paths = (
        OpenMeteoBatchIngestion()
        .ingest_hourly_locations(
            locations=locations,
            target_hour=context[
                "data_interval_start"
            ],
        )
    )

    return (
        f"{len(locations)} stations; "
        f"{len(paths)} Bronze files"
    )


with DAG(
    dag_id="hourly_ingestion",
    description=(
        "Hourly approved ESIOS generation, "
        "AEMET observations and Open-Meteo ingestion."
    ),
    schedule="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=[
        "esios",
        "aemet",
        "open_meteo",
        "bronze",
        "hourly",
    ],
) as dag:

    for indicator_id, dataset in (
        ESIOS_HOURLY_INDICATORS.items()
    ):
        PythonOperator(
            task_id=f"esios_{indicator_id}",
            python_callable=(
                ingest_esios_hourly_indicator
            ),
            op_kwargs={
                "indicator_id": indicator_id,
                "dataset": dataset,
            },
        )

    PythonOperator(
        task_id="aemet_current_observations",
        python_callable=ingest_aemet_observations,
    )

    PythonOperator(
        task_id="open_meteo_hourly_all_stations",
        python_callable=ingest_open_meteo_hourly,
    )
