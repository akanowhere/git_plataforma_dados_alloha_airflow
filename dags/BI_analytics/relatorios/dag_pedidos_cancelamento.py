from airflow.decorators import dag
from utils.airflow_utils import AirflowUtil
from utils.bi_utils import (
    DEFAULT_ARGS_DAGS,
    standard_task,
    task_group
)

@dag(
    dag_id="Relatorio_pedidos_cancelamento",
    tags=["relatorios", "dbt", "gold"],
    schedule_interval=None,
    **DEFAULT_ARGS_DAGS
)
def dag_pedidos_cancelamento() -> None:
    """
    DAG para executar processos relacionados ao relatórios Pedidos de Cancelamento.
    """    
    (
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> task_group("Relatorio_pedidos_cancelamento", 'gold.relatorios', 'relatorios/PedidosCancelamento') 
        >>  standard_task("end_task", "Task que indica o fim do pipeline.")
    )

dag_pedidos_cancelamento()