from datetime import timedelta

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Cobranca",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None,
    start_date=days_ago(1),
    on_failure_callback=notify_teams,
    catchup=False,
    concurrency=5,
    tags=["cobranca", "dbt", "gold"],
)
def dag_cobranca() -> None:
    """
    DAG para executar processos relacionados a arrecadacao.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    # Sensor externo para esperar pelo término da DAG air_ingestion
    wait_for_air_ingestion = ExternalTaskSensor(
        task_id="wait_for_air_ingestion",
        external_dag_id="Ingestion_daily_air_pipeline",
        external_task_id="end_task",  # Assumindo "end_task" como a última tarefa na DAG air_ingestion
        check_existence=True,
        timeout=14400,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
            dt, "Ingestion_daily_air_pipeline"
        ),
        mode="reschedule",
    )

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Criação do grupo de tarefas DBT para a gold
    gold_cobranca = AirflowUtil().create_dbt_task_group(
        group_id="gold_cobranca",
        profile_name="cobranca",
        catalog=catalog_gold,
        schema="cobranca",
        select_paths=["path:models/gold/cobranca"],
        exclude=[""],
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    # Definição do fluxo da DAG
    start_task = start_task()
    end_task = end_task()

    start_task >> wait_for_air_ingestion >> gold_cobranca >> end_task


dag_cobranca()
