"""
Historical end-to-end reload DAG.

Orchestrates:
APIs -> Bronze -> Silver -> Gold

The historical date range is supplied at DAG execution time.
Overwrite/startup persistence policy is intentionally outside
the scope of this DAG.
"""

from datetime import date

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.utils.dates import days_ago

from ingestion.orchestration.historical_reload import (
    ingest_aemet_current,
    ingest_esios_hourly,
    ingest_esios_monthly,
    ingest_masters,
    ingest_open_meteo,
    validate_date_range,
)


SPARK_ENV = {
    "PYTHONPATH": "/opt/spark/jobs",
}


def _get_historical_range(**context):
    params = context["params"]

    start_text = params.get("fecha_inicio")
    end_text = params.get("fecha_fin")

    if not start_text or not end_text:
        raise ValueError(
            "fecha_inicio and fecha_fin are required."
        )

    start_date = date.fromisoformat(
        start_text
    )
    end_date = date.fromisoformat(
        end_text
    )

    validate_date_range(
        start_date,
        end_date,
    )

    return start_date, end_date


def ingest_esios_hourly_task(**context):
    start_date, end_date = (
        _get_historical_range(**context)
    )

    return ingest_esios_hourly(
        start_date,
        end_date,
    )


def ingest_esios_monthly_task(**context):
    start_date, end_date = (
        _get_historical_range(**context)
    )

    return ingest_esios_monthly(
        start_date,
        end_date,
    )


def ingest_open_meteo_task(**context):
    start_date, end_date = (
        _get_historical_range(**context)
    )

    return ingest_open_meteo(
        start_date,
        end_date,
    )


with DAG(
    dag_id="historical_reload",
    description=(
        "Historical APIs -> Bronze -> Silver -> Gold reload."
    ),
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    params={
        "fecha_inicio": Param(
            None,
            type=[
                "null",
                "string",
            ],
            description=(
                "Historical start date YYYY-MM-DD"
            ),
        ),
        "fecha_fin": Param(
            None,
            type=[
                "null",
                "string",
            ],
            description=(
                "Historical end date YYYY-MM-DD"
            ),
        ),
    },
    tags=[
        "historical",
        "bronze",
        "silver",
        "gold",
        "e2e",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    ingest_master_data = PythonOperator(
        task_id="ingest_master_data",
        python_callable=ingest_masters,
    )

    ingest_hourly_energy = PythonOperator(
        task_id="ingest_esios_hourly",
        python_callable=ingest_esios_hourly_task,
    )

    ingest_monthly_capacity = PythonOperator(
        task_id="ingest_esios_monthly",
        python_callable=ingest_esios_monthly_task,
    )

    ingest_weather = PythonOperator(
        task_id="ingest_open_meteo_historical",
        python_callable=ingest_open_meteo_task,
    )

    ingest_current_weather = PythonOperator(
        task_id="ingest_aemet_current",
        python_callable=ingest_aemet_current,
    )

    bronze_complete = EmptyOperator(
        task_id="bronze_complete",
    )

    silver_create = SparkSubmitOperator(
        task_id="silver_create_tables",
        conn_id="spark_default",
        application=(
            "/opt/spark/jobs/silver/"
            "create_tables.py"
        ),
        env_vars=SPARK_ENV,
    )

    silver_write = SparkSubmitOperator(
        task_id="silver_write",
        conn_id="spark_default",
        application=(
            "/opt/spark/jobs/silver/"
            "write_silver.py"
        ),
        env_vars=SPARK_ENV,
    )

    gold_create = SparkSubmitOperator(
        task_id="gold_create_tables",
        conn_id="spark_default",
        application=(
            "/opt/spark/jobs/gold/"
            "create_tables.py"
        ),
        env_vars=SPARK_ENV,
    )

    gold_write = SparkSubmitOperator(
        task_id="gold_write",
        conn_id="spark_default",
        application=(
            "/opt/spark/jobs/gold/"
            "write_gold.py"
        ),
        env_vars=SPARK_ENV,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> [
        ingest_master_data,
        ingest_hourly_energy,
        ingest_monthly_capacity,
        ingest_current_weather,
    ]

    ingest_master_data >> ingest_weather

    [
        ingest_master_data,
        ingest_hourly_energy,
        ingest_monthly_capacity,
        ingest_weather,
        ingest_current_weather,
    ] >> bronze_complete

    (
        bronze_complete
        >> silver_create
        >> silver_write
        >> gold_create
        >> gold_write
        >> end
    )
