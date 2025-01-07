from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Dims_teste",
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
    max_active_tasks=4,
    concurrency=5,
    tags=["dims", "base", "chamados", "contato", "venda", "dbt", "gold"],
)
def dag_dims() -> None:
    """
    DAG para executar as dimensões.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"



    # Criação do grupo de tarefas DBT para a gold
    

    # dims_venda = AirflowUtil().create_dbt_task_group(
    #     group_id="dims_venda",
    #     profile_name="dims_venda",
    #     catalog=catalog_gold,
    #     schema="venda",
    #     select_paths=["path:models/gold/venda"],
    #     exclude=[""],
    # )

    dims_chamados = AirflowUtil().create_dbt_task_group(
        group_id="dims_chamados",
        profile_name="dims_chamados",
        catalog=catalog_gold,
        schema="chamados",
        select_paths=["path:models/gold/chamados"],
        exclude=[""],
    )

    

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    end_task = end_task()

    start_task >> dims_chamados >> end_task

dag_dims()
