"""
Airflow DAG for 5-minute REE / ESIOS ingestion.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.esios.ingest import EsiosIngestion


ESIOS_5MIN_INDICATORS = {
    1293: "demanda_real",
    10004: "demanda_real_suma_generacion",
    2038: "generacion_treal_eolica_nacional",
    2039: "generacion_treal_nuclear_nacional",
    2040: "generacion_treal_carbon_nacional",
    2041: "generacion_treal_ciclo_combinado_nacional",
    2042: "generacion_treal_hidraulica_nacional",
    2044: "generacion_treal_solar_fotovoltaica_nacional",
    2045: "generacion_treal_solar_termica_nacional",
    2046: "generacion_treal_termica_renovable_nacional",
    2051: "generacion_treal_cogeneracion_residuos_nacional",
    2065: "generacion_treal_consumo_bombeo_nacional",
}


def ingest_esios_indicator(
    *,
    indicator_id: int,
    dataset: str,
    **context,
) -> str:
    """
    Ingest one ESIOS indicator for the current Airflow data interval.
    """

    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    # ESIOS treats the end timestamp as inclusive.
    # Subtracting one second prevents overlap between consecutive runs.
    request_end = data_interval_end - timedelta(seconds=1)

    ingestion = EsiosIngestion()

    return ingestion.ingest_incremental(
        indicator_id=indicator_id,
        dataset=dataset,
        start_date=data_interval_start,
        end_date=request_end,
    )


with DAG(
    dag_id="esios_5min",
    description="Ingest 5-minute REE / ESIOS indicators into Bronze.",
    schedule="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["esios", "bronze", "5min"],
) as dag:

    for indicator_id, dataset in ESIOS_5MIN_INDICATORS.items():
        PythonOperator(
            task_id=f"ingest_{indicator_id}",
            python_callable=ingest_esios_indicator,
            op_kwargs={
                "indicator_id": indicator_id,
                "dataset": dataset,
            },
        )