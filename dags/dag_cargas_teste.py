from datetime import datetime

from airflow.decorators import dag, task
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="dag_teste",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    schedule_interval=None,
    start_date=datetime(
        datetime.today().year, datetime.today().month, datetime.today().day
    ),
    on_failure_callback=notify_teams,
    catchup=False,
    max_active_runs=2,
    concurrency=4,
    tags=["dbt", "teste"],
)
def dag_teste() -> None:
    """
    DAG para executar as dimensões e as fatos.
    """
    catalog_gold, catalog_silver = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    gold_chamados = AirflowUtil().create_dbt_task_group(
        group_id="chamados",
        profile_name="chamados",
        catalog=catalog_gold,
        schema="chamados",
        select_paths=["path:models/gold/chamados/"],
        exclude=[""],
    )

    dim_contrato_reajuste = AirflowUtil().create_dbt_task_group(
        group_id="dim_contrato_reajuste",
        profile_name="dim_contrato_reajuste",
        catalog=catalog_gold,
        schema="base",
        select_paths=["path:models/gold/base/dim_contrato_reajuste.sql"],
        exclude=[""],
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    end_task = end_task()

    start_task >> gold_chamados >> dim_contrato_reajuste >> end_task


dag_teste()
