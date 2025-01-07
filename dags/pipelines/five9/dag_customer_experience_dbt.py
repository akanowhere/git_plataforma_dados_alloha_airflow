from datetime import timedelta

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Experiencia_cliente",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None,
    on_failure_callback=notify_teams,
    catchup=False,
    tags=["dims", "air", "five9", "experiencia_cliente", "dbt", "gold"],
)
def dag_customer_experience() -> None:
    """DAG para executar os relatorios da àrea de experiência do cliente."""
    gold_catalog, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Sensor externo para esperar pelo término da DAG do AIR
    wait_for_air_ingestion = ExternalTaskSensor(
        task_id="wait_for_air_ingestion",
        external_dag_id="Ingestion_daily_air_pipeline",
        external_task_id="end_task",
        check_existence=True,
        timeout=14400,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
            dt, "Ingestion_daily_air_pipeline"
        ),
        mode="reschedule",
    )

    # Sensor externo para esperar pelo término da DAG do Five9
    wait_for_five9_ingestion = ExternalTaskSensor(
        task_id="wait_for_five9_ingestion",
        external_dag_id="Ingestion_daily_five9_pipeline",
        external_task_id="end_task",
        check_existence=True,
        timeout=14400,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
            dt, "Ingestion_daily_five9_pipeline"
        ),
        mode="reschedule",
    )

    # Criação do grupo de tarefas DBT para a gold
    to_gold_tasks = AirflowUtil().create_dbt_task_group(
        group_id="to_gold_tasks",
        profile_name="to_gold_tasks",
        catalog=gold_catalog,
        schema="experiencia_cliente",
        select_paths=["path:models/gold/experiencia_cliente"],
        exclude=[""],
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    # Definição do fluxo da DAG
    start_task = start_task()
    end_task = end_task()

    (
        start_task
        >> [wait_for_five9_ingestion, wait_for_air_ingestion]
        >> to_gold_tasks
        >> end_task
    )


dag_customer_experience()
