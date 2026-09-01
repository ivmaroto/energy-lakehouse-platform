"""
Historical end-to-end reload DAG.

Orchestrates:

    persistence policy
        -> APIs
        -> Bronze
        -> Silver
        -> Gold

Runtime parameters:

    fecha_inicio
    fecha_fin
    sobreescribir_datos
    eliminar_historial_completo

Persistence policy:

    Normal load
        -> preserve existing Bronze/Silver/Gold
        -> ingest missing historical coverage

    Range overwrite
        -> delete requested historical range
        -> preserve master datasets
        -> rebuild requested interval

    Full historical deletion
        -> takes priority over range overwrite
        -> delete active Bronze/Silver/Gold
        -> rebuild masters
        -> load only requested interval

AEMET current observations are deliberately excluded from
historical reload.
"""


from datetime import date

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from ingestion.orchestration.historical_reload import (
    delete_all_bronze,
    delete_bronze_range,
    delete_all_warehouse_residuals,
    ingest_esios_hourly,
    ingest_esios_monthly,
    ingest_masters,
    ingest_open_meteo,
    validate_date_range,
)


# ============================================================================
# Spark environment
# ============================================================================

SPARK_ENV = {
    "PYTHONPATH": "/opt/spark/jobs",
    "SPARK_CONF_DIR": "/tmp/spark-conf",
}

HISTORICAL_WRITE_ENV = {
    **SPARK_ENV,
    "LAKEHOUSE_WRITE_POLICY": "insert-only",
}


# ============================================================================
# Runtime parameter helpers
# ============================================================================

def _get_historical_range(
    **context,
) -> tuple[date, date]:
    params = context["params"]

    start_text = params.get(
        "fecha_inicio"
    )

    end_text = params.get(
        "fecha_fin"
    )

    if (
        not start_text
        or not end_text
    ):
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

    return (
        start_date,
        end_date,
    )


def _get_boolean_param(
    context,
    name: str,
) -> bool:
    value = context[
        "params"
    ].get(
        name,
        False,
    )

    if not isinstance(
        value,
        bool,
    ):
        raise ValueError(
            f"{name} must be boolean."
        )

    return value


def _get_persistence_policy(
    **context,
) -> tuple[date, date, bool, bool]:
    """
    Return the validated runtime persistence policy.

    Full deletion takes priority over range overwrite.
    """

    start_date, end_date = (
        _get_historical_range(
            **context
        )
    )

    overwrite = (
        _get_boolean_param(
            context,
            "sobreescribir_datos",
        )
    )

    full_delete = (
        _get_boolean_param(
            context,
            "eliminar_historial_completo",
        )
    )

    return (
        start_date,
        end_date,
        overwrite,
        full_delete,
    )


# ============================================================================
# Bronze persistence policy
# ============================================================================

def apply_bronze_policy_task(
    **context,
):
    (
        start_date,
        end_date,
        overwrite,
        full_delete,
    ) = _get_persistence_policy(
        **context
    )

    # Full deletion has priority.
    if full_delete:
        deleted = (
            delete_all_bronze()
        )

        return {
            "mode": "full",
            "deleted": deleted,
        }

    if overwrite:
        deleted = (
            delete_bronze_range(
                start_date,
                end_date,
            )
        )

        return {
            "mode": "range",
            "deleted": deleted,
        }

    print(
        "BRONZE PERSISTENCE POLICY = PRESERVE"
    )

    return {
        "mode": "preserve",
        "deleted": 0,
    }


# ============================================================================
# Iceberg deletion branch
# ============================================================================

def select_iceberg_delete_task(
    **context,
) -> str:
    (
        _,
        _,
        overwrite,
        full_delete,
    ) = _get_persistence_policy(
        **context
    )

    if full_delete:
        return (
            "delete_silver_gold_full"
        )

    if overwrite:
        return (
            "delete_silver_gold_range"
        )

    return (
        "skip_silver_gold_delete"
    )


# ============================================================================
# Bronze ingestion tasks
# ============================================================================

def ingest_esios_hourly_task(
    **context,
):
    start_date, end_date = (
        _get_historical_range(
            **context
        )
    )

    return ingest_esios_hourly(
        start_date,
        end_date,
    )


def ingest_esios_monthly_task(
    **context,
):
    start_date, end_date = (
        _get_historical_range(
            **context
        )
    )

    return ingest_esios_monthly(
        start_date,
        end_date,
    )


def ingest_open_meteo_task(
    **context,
):
    start_date, end_date = (
        _get_historical_range(
            **context
        )
    )

    return ingest_open_meteo(
        start_date,
        end_date,
    )


# ============================================================================
# DAG
# ============================================================================

with DAG(
    dag_id="historical_reload",
    description=(
        "Historical APIs -> Bronze -> Silver -> Gold reload."
    ),
    schedule=None,
    start_date=days_ago(
        1
    ),
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
        "sobreescribir_datos": Param(
            False,
            type="boolean",
            description=(
                "Delete and rebuild only the requested "
                "historical interval."
            ),
        ),
        "eliminar_historial_completo": Param(
            False,
            type="boolean",
            description=(
                "Delete the complete active Bronze, "
                "Silver and Gold layers before rebuilding. "
                "Takes priority over sobreescribir_datos."
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

    # ========================================================================
    # Start
    # ========================================================================

    start = EmptyOperator(
        task_id="start",
    )

    # ========================================================================
    # Persistence policy
    # ========================================================================

    apply_bronze_policy = (
        PythonOperator(
            task_id=(
                "apply_bronze_persistence_policy"
            ),
            python_callable=(
                apply_bronze_policy_task
            ),
        )
    )

    select_iceberg_delete = (
        BranchPythonOperator(
            task_id=(
                "select_iceberg_delete_policy"
            ),
            python_callable=(
                select_iceberg_delete_task
            ),
        )
    )

    # ------------------------------------------------------------------------
    # Full Silver/Gold deletion
    # ------------------------------------------------------------------------

    delete_silver_gold_full = (
        SparkSubmitOperator(
            task_id=(
                "delete_silver_gold_full"
            ),
            conn_id="spark_default",
            application=(
                "/opt/spark/jobs/maintenance/"
                "delete_historical_data.py"
            ),
            application_args=[
                "--mode",
                "full",
            ],
            env_vars=SPARK_ENV,
        )
    )

    cleanup_silver_gold_physical_full = (
        PythonOperator(
            task_id="cleanup_silver_gold_physical_full",
            python_callable=delete_all_warehouse_residuals,
        )
    )

    # ------------------------------------------------------------------------
    # Range Silver/Gold deletion
    # ------------------------------------------------------------------------

    delete_silver_gold_range = (
        SparkSubmitOperator(
            task_id=(
                "delete_silver_gold_range"
            ),
            conn_id="spark_default",
            application=(
                "/opt/spark/jobs/maintenance/"
                "delete_historical_data.py"
            ),
            application_args=[
                "--mode",
                "range",
                "--start-date",
                "{{ params.fecha_inicio }}",
                "--end-date",
                "{{ params.fecha_fin }}",
            ],
            env_vars=SPARK_ENV,
        )
    )

    # ------------------------------------------------------------------------
    # No deletion
    # ------------------------------------------------------------------------

    skip_silver_gold_delete = (
        EmptyOperator(
            task_id=(
                "skip_silver_gold_delete"
            ),
        )
    )

    persistence_policy_complete = (
        EmptyOperator(
            task_id=(
                "persistence_policy_complete"
            ),
            trigger_rule=(
                TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            ),
        )
    )

    # ========================================================================
    # Bronze ingestion
    # ========================================================================

    ingest_master_data = (
        PythonOperator(
            task_id="ingest_master_data",
            python_callable=ingest_masters,
        )
    )

    ingest_hourly_energy = (
        PythonOperator(
            task_id="ingest_esios_hourly",
            python_callable=(
                ingest_esios_hourly_task
            ),
        )
    )

    ingest_monthly_capacity = (
        PythonOperator(
            task_id="ingest_esios_monthly",
            python_callable=(
                ingest_esios_monthly_task
            ),
        )
    )

    ingest_weather = (
        PythonOperator(
            task_id=(
                "ingest_open_meteo_historical"
            ),
            python_callable=(
                ingest_open_meteo_task
            ),
        )
    )

    bronze_complete = (
        EmptyOperator(
            task_id="bronze_complete",
        )
    )

    # ========================================================================
    # Silver
    # ========================================================================

    silver_create = (
        SparkSubmitOperator(
            task_id="silver_create_tables",
            conn_id="spark_default",
            application=(
                "/opt/spark/jobs/silver/"
                "create_tables.py"
            ),
            env_vars=SPARK_ENV,
        )
    )

    silver_write = (
        SparkSubmitOperator(
            task_id="silver_write",
            conn_id="spark_default",
            application=(
                "/opt/spark/jobs/silver/"
                "write_silver.py"
            ),
            env_vars=HISTORICAL_WRITE_ENV,
        )
    )

    # ========================================================================
    # Gold
    # ========================================================================

    gold_create = (
        SparkSubmitOperator(
            task_id="gold_create_tables",
            conn_id="spark_default",
            application=(
                "/opt/spark/jobs/gold/"
                "create_tables.py"
            ),
            env_vars=SPARK_ENV,
        )
    )

    gold_write = (
        SparkSubmitOperator(
            task_id="gold_write",
            conn_id="spark_default",
            application=(
                "/opt/spark/jobs/gold/"
                "write_gold.py"
            ),
            env_vars=HISTORICAL_WRITE_ENV,
        )
    )

    # ========================================================================
    # End
    # ========================================================================

    end = EmptyOperator(
        task_id="end",
    )

    # ========================================================================
    # Dependencies
    # ========================================================================

    (
        start
        >> apply_bronze_policy
        >> select_iceberg_delete
    )

    select_iceberg_delete >> [
        delete_silver_gold_full,
        delete_silver_gold_range,
        skip_silver_gold_delete,
    ]

    delete_silver_gold_full >> cleanup_silver_gold_physical_full

    [
        cleanup_silver_gold_physical_full,
        delete_silver_gold_range,
        skip_silver_gold_delete,
    ] >> persistence_policy_complete

    persistence_policy_complete >> [
        ingest_master_data,
        ingest_hourly_energy,
        ingest_monthly_capacity,
    ]

    ingest_master_data >> ingest_weather

    [
        ingest_master_data,
        ingest_hourly_energy,
        ingest_monthly_capacity,
        ingest_weather,
    ] >> bronze_complete

    (
        bronze_complete
        >> silver_create
        >> silver_write
        >> gold_create
        >> gold_write
        >> end
    )
