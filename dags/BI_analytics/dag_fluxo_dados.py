from airflow.decorators import dag
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil
from utils.bi_utils import (
    DEFAULT_ARGS_DAGS,
    standard_task,
    task_group,
)

@dag(
    dag_id="Fluxo_dados",
    schedule_interval="0 8 * * *",
    tags=["fluxo_dados", "dbt", "silver", "views", "powerbi"],
    **DEFAULT_ARGS_DAGS
)
def dag_fluxo_dados() -> None:
    """
    DAG para criar as view utilizadas no fluxo de dados do Power BI
    """
    (
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> task_group("fluxo_dados",'silver.fluxo_dados', 'fluxo_dados') 
        >> standard_task("end_task", "Task que indica o fim do pipeline.")
    )

dag_fluxo_dados()
