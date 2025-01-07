from airflow.decorators import dag
from utils.airflow_utils import AirflowUtil
from utils.bi_utils import (
    DEFAULT_ARGS_DAGS,
    standard_task,
    task_group
)

@dag(
    dag_id="Fpd_spd",
    schedule_interval=None,
    tags=["fpd", "spd", "dbt", "gold", "powerbi"],
    **DEFAULT_ARGS_DAGS
)
def dag_fpd_spd() -> None:
    """
    DAG para criar as tabela do fpd
    """
    (
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> task_group("fpd_spd",'gold.fpd','fpd_spd/create') 
        >> standard_task("end_task", "Task que indica o fim do pipeline.")
    )

dag_fpd_spd()
