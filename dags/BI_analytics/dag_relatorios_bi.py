from airflow.decorators import dag
from utils.airflow_utils import AirflowUtil
from utils.bi_utils import (
    DEFAULT_ARGS_DAGS,
    standard_task,
    trigger_dag,
    create_databricks_task,
    get_job_id,
    sleep_task
)

dag_ids_to_trigger = [
    'Relatorio_basao', 
    'Relatorio_altas_baixas',
    'Relatorio_baixas_altas',     
    'Relatorio_contratos_para_suspensao', 
    'Relatorio_contratos_suspensos', 
    'Relatorio_descontos_a_vencer', 
    'Relatorio_pagamentos_negativados',
    'Relatorio_contratos_ott',
    'Relatorio_pedidos_cancelamento'
]

@dag(
    **DEFAULT_ARGS_DAGS,
    schedule_interval=None,
    dag_id="Relatorios",
    tags=["relatorios", "dbt", "gold"],
)
def dag_relatorios() -> None:
    """
    DAG para executar processos relacionados aos relatórios de BI.
    """
    (
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> [trigger_dag(dag_id) for dag_id in dag_ids_to_trigger[:2]] 
        >> standard_task("join", "Task pra fazer um join pipeline.")
        >> [trigger_dag(dag_id) for dag_id in dag_ids_to_trigger[2:4]] 
        >> standard_task("join2", "Task pra fazer um join pipeline.")
        >> [trigger_dag(dag_id) for dag_id in dag_ids_to_trigger[4:6]] 
        >> standard_task("join3", "Task pra fazer um join pipeline.")
        >> [trigger_dag(dag_id) for dag_id in dag_ids_to_trigger[6:8]]
        >> standard_task("join4", "Task pra fazer um join pipeline.")
        >> [trigger_dag(dag_id) for dag_id in dag_ids_to_trigger[8:9]]
        >> standard_task("join5", "Task pra fazer um join pipeline.")
        >> sleep_task(20)
        >> create_databricks_task('salvar_relatorios', get_job_id('relatorios')) 
        >> standard_task("end_task", "Task que indica o fim do pipeline.")
    )
dag_relatorios()
