"""
Airflow DAG for monthly REE / ESIOS installed-capacity ingestion.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.esios.ingest import EsiosIngestion


ESIOS_MONTHLY_INDICATORS = {
    1475: "potencia_instalada_hidraulica",
    1485: "potencia_instalada_eolica",
    1486: "potencia_instalada_solar_fotovoltaica",
    1487: "potencia_instalada_solar_termica",
    10302: "potencia_instalada_total_renovable",
    1477: "potencia_instalada_nuclear",
    1478: "potencia_instalada_carbon",
    1483: "potencia_instalada_ciclo_combinado",
    1488: "potencia_instalada_otras_renovables",
}


def ingest_esios_monthly_indicator(
    *,
    indicator_id: int,
    dataset: str,
    **context,
) -> str:
    """
    Ingest one monthly ESIOS installed-capacity indicator
    for the Airflow data interval.
    """

    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    # ESIOS treats the end timestamp as inclusive.
    request_end = data_interval_end - timedelta(seconds=1)

    ingestion = EsiosIngestion()

    return ingestion.ingest_incremental(
        indicator_id=indicator_id,
        dataset=dataset,
        start_date=data_interval_start,
        end_date=request_end,
    )


with DAG(
    dag_id="monthly_ingestion",
    description="Monthly ESIOS installed-capacity ingestion into Bronze.",
    schedule="@monthly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["esios", "bronze", "monthly"],
) as dag:

    for indicator_id, dataset in ESIOS_MONTHLY_INDICATORS.items():
        PythonOperator(
            task_id=f"esios_{indicator_id}",
            python_callable=ingest_esios_monthly_indicator,
            op_kwargs={
                "indicator_id": indicator_id,
                "dataset": dataset,
            },
        )