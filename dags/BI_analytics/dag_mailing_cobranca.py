from airflow.decorators import dag
from utils.bi_utils import (
    DEFAULT_ARGS_DAGS,
    standard_task,
    task_group,
    create_databricks_task
)

@dag(
    dag_id="Mailing_cobranca",
    schedule_interval=None,
    tags=[
        "mailing_cobranca","gold","daily",
        "analiticos","contatos","serasa",
    ],
    **DEFAULT_ARGS_DAGS
)
def dag_mailing_cobranca() -> None:
    """
        DAG para executar processos relacionados ao mailing da cobrança.
    """ 
    (   
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> task_group('mailing_contatos','gold.mailing','mailing_cobranca/contatos') 
        >> task_group('mailing_create','gold.mailing','mailing_cobranca/analitico','mailing_cobranca/analitico/intermediate') 
        >> task_group('mailing_create_serasa','gold.mailing','mailing_cobranca/serasa','mailing_cobranca/serasa/intermediate') 
        >> task_group('report_reversao','gold.mailing','mailing_cobranca/reports') 
        >> create_databricks_task('entregas_mailing',123123832735899)
        >> create_databricks_task('reports_mailing',4250963425433)
        >> standard_task("end_task", "Task que indica o fim do pipeline.")
    )
dag_mailing_cobranca()