from airflow.decorators import dag
from utils.airflow_utils import AirflowUtil
from utils.bi_utils import (
    DEFAULT_ARGS_DAGS,
    standard_task,
    task_group
)

@dag(
    dag_id="Churn_voluntario",
    schedule_interval=None,
    tags=["churn", "Voluntario", "dbt", "gold", "powerbi"],
    **DEFAULT_ARGS_DAGS
)
def dag_churn_vol() -> None:
    """
    DAG para criar as tabela do fpd
    """
    (
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> task_group("tbl_intermediarias",'gold.churn_voluntario','churn_vol/intermediate') 
        >> task_group("tbl_chun_vol",'gold.churn_voluntario','churn_vol/create') 
        >> standard_task("end_task", "Task que indica o fim do pipeline.")
    )

dag_churn_vol()
