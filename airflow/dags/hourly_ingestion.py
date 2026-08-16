"""
Airflow DAG for hourly ingestion.

Includes:
- Provincial hourly REE / ESIOS generation indicators.
- Peninsular hourly REE / ESIOS demand indicators.
- AEMET conventional observations.
"""

from datetime import timedelta

from ingestion.common.esios_config import load_esios_indicators

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.aemet.ingest import AemetIngestion
from ingestion.esios.ingest import EsiosIngestion


ESIOS_HOURLY_INDICATORS = load_esios_indicators("hourly")


def ingest_esios_hourly_indicator(
    *,
    indicator_id: int,
    dataset: str,
    **context,
) -> str:
    """
    Ingest one hourly ESIOS indicator for the Airflow data interval.
    """

    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    # ESIOS includes the end timestamp.
    # Avoid overlap with the next hourly execution.
    request_end = data_interval_end - timedelta(seconds=1)

    ingestion = EsiosIngestion()

    return ingestion.ingest_incremental(
        indicator_id=indicator_id,
        dataset=dataset,
        start_date=data_interval_start,
        end_date=request_end,
    )


def ingest_aemet_observations() -> str:
    """
    Retrieve current conventional observations from AEMET.
    """

    ingestion = AemetIngestion()

    return ingestion.ingest_current_observations()


with DAG(
    dag_id="hourly_ingestion",
    description=(
        "Hourly ESIOS provincial/demand and "
        "AEMET conventional observation ingestion."
    ),
    schedule="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["esios", "aemet", "bronze", "hourly"],
) as dag:

    for indicator_id, dataset in ESIOS_HOURLY_INDICATORS.items():
        PythonOperator(
            task_id=f"esios_{indicator_id}",
            python_callable=ingest_esios_hourly_indicator,
            op_kwargs={
                "indicator_id": indicator_id,
                "dataset": dataset,
            },
        )

    PythonOperator(
        task_id="aemet_current_observations",
        python_callable=ingest_aemet_observations,
    )