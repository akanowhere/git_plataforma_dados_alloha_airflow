from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Fatos_hourly",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval="00 12-23 * * *",
    start_date=days_ago(1),
    on_failure_callback=notify_teams,
    catchup=False,
    max_active_tasks=4,
    concurrency=5,
    tags=["fatos", "hourly", "dbt", "gold"],
)
def dag_fatos_hourly() -> None:
    """
    DAG para executar as fatos a cada 1 hora.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    fatos = AirflowUtil().create_dbt_task_group(
        group_id="fatos",
        profile_name="fatos",
        catalog=catalog_gold,
        schema="base",
        select_paths=[
            "path:models/gold/base/fato_cancelamento.sql",
            "path:models/gold/base/fato_ativacao.sql",
            "path:models/gold/base/fato_primeira_ativacao.sql",
            "path:models/gold/venda/fato_venda.sql",
        ],
        exclude=[""],
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    end_task = end_task()

    (start_task >> fatos >> end_task)


dag_fatos_hourly()
