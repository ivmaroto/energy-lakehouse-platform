"""
Airflow DAG for hourly ingestion.

Includes:
- Provincial hourly REE / ESIOS generation indicators.
- Peninsular hourly REE / ESIOS demand indicators.
- AEMET conventional observations.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.aemet.ingest import AemetIngestion
from ingestion.esios.ingest import EsiosIngestion


ESIOS_HOURLY_INDICATORS = {
    1159: "generacion_medida_eolica_terrestre",
    1161: "generacion_medida_solar_fotovoltaica",
    1162: "generacion_medida_solar_termica",
    10035: "generacion_medida_hidraulica",
    1153: "generacion_medida_nuclear",
    1156: "generacion_medida_ciclo_combinado",
    1158: "generacion_medida_gas_natural_turbina_vapor",
    1164: "generacion_medida_gas_natural_cogeneracion",
    10036: "generacion_medida_carbon",
    10041: "generacion_medida_otras_renovables",
    10043: "generacion_medida_total",
    10195: "generacion_medida_total_tipo_produccion",
    1193: "demanda_en_consumo",
    10267: "demanda_medida_discriminacion_horaria_total",
}


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