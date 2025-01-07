from airflow.decorators import dag
from utils.airflow_utils import AirflowUtil
from utils.bi_utils import (
    DEFAULT_ARGS_DAGS,
    standard_task,
    task_group
)

@dag(
    dag_id="Relatorio_contratos_suspensos",
    tags=["relatorios", "dbt", "gold"],
    schedule_interval=None,
    **DEFAULT_ARGS_DAGS
)
def dag_altas_baixas() -> None:
    """
    DAG para executar processos relacionados ao relatórios altas_baixas.
    """    
    (
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> task_group("Relatorio_ContratoSuspensos", 'gold.relatorios', 'relatorios/ContratoSuspensos') 
        >>  standard_task("end_task", "Task que indica o fim do pipeline.")
    )

dag_altas_baixas()