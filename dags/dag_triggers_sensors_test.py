import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.airflow_utils import AirflowUtil
from include.teams_notifications import notify_teams

os.environ["env"] = Variable.get("env")
@dag(
    dag_id="Teste_triggers_sensors",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    on_failure_callback=notify_teams,
    schedule_interval=None,
    catchup=False,
    max_active_tasks=5,
    tags=["test"],
)
def triggers_sensors_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Sensor externo para esperar pelo término da DAG dim (dim_faturas_mailing depende de 3 dims)
    wait_for_assine_ingestion = ExternalTaskSensor(
        task_id="wait_for_assine_ingestion",
        external_dag_id="Ingestion_daily_assine_pipeline",
        external_task_id="end_task",
        check_existence=True,
        timeout=180,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
            dt, "Ingestion_daily_assine_pipeline"
        ),
        mode="reschedule",
    )
    
    wait_for_air_ingestion = ExternalTaskSensor(
        task_id="wait_for_air_ingestion",
        external_dag_id="Ingestion_daily_air_pipeline",
        external_task_id="end_task",
        check_existence=True,
        timeout=180,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
            dt, "Ingestion_daily_air_pipeline"
        ),
        mode="reschedule",
    )
    
    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()

    end_task = end_task()


    (
        start_task
        >> [wait_for_assine_ingestion, wait_for_air_ingestion] 
        >> end_task 
    )


triggers_sensors_dag()
