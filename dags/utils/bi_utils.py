import os
from time import sleep
from datetime import timedelta, datetime
from typing import Callable, List, Dict
from include.teams_notifications import notify_teams
from airflow.decorators import task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.airflow_utils import AirflowUtil
from airflow.models import DagRun



DEFAULT_ARGS_DAGS = {
    "default_args": {
        "owner": "BI_analytics",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    'start_date':datetime(2024, 6, 1),
    "on_failure_callback": notify_teams,
    "catchup": False,
    "max_active_tasks": 4,
    "concurrency": 5,
}

# Dicionário de mapeamento de jobs
jobs_databricks = {
    'dev': {
        'relatorios': 670514862466524,    
    },
    'prd': {
        'relatorios': 725858018014571,
    }
}

def get_job_id(job_name):
    """
    Função para obter o ID do job baseado no nome e ambiente.
    """
    env = Variable.get("env")
    return jobs_databricks.get(env, {}).get(job_name)

def get_script_function(path_script: str):
    """
    Função que executa um script Python localizado no diretório especificado.

    Args:
        path_script (str): O caminho onde o script Python (`main.py`) está localizado.
        path_update_queries (str): O caminho onde as queries de update estão localizadas.
        order_queries (list): Lista com a ordem de execução das queries.
    """
    from logging import info, error
    from os import chdir 
    from sys import path

    info('Adiciona o diretório ao caminho do sistema para importar módulos personalizados')
    path.append("/opt/airflow/scripts")
    info('Diretório principal dos scripts: /opt/airflow/scripts')
    info('Muda o diretório de trabalho atual para o diretório do script')
    chdir("/opt/airflow/scripts")

    info(f'Executando script {path_script}/main.py')
    exec(open(f'{path_script}/main.py').read())

def get_requirements(path_script: str) -> List[str]:
    """
    Lê e retorna a lista de requisitos de um arquivo `requirements.txt` localizado no diretório especificado.

    Args:
        path_script (str): O caminho onde o arquivo `requirements.txt` está localizado.

    Returns:
        List[str]: Uma lista de strings, cada uma representando um requisito do arquivo `requirements.txt`.
    """
    from logging import info
    from os import chdir 
    from sys import path
    info('Adiciona o diretório ao caminho do sistema para importar módulos personalizados')
    path.append("/opt/airflow/scripts")
    info('diretorio principal dos scripts: /opt/airflow/scripts')
    script_directory = "/opt/airflow/scripts"

    info('Muda o diretório de trabalho atual para o diretório do script')
    chdir(script_directory)
    info('Lendo requirements.txt')
    with open(f'{path_script}/requirements.txt', 'r', encoding='utf-16') as requirements:
        req = requirements.readlines()
    info('Retornando lista de pacotes')
    return [line.strip() for line in req]

def standard_task(task_name: str, task_description: str) -> Callable:
    """
    Cria uma função de tarefa padrão para o Airflow com o nome e a descrição fornecidos.

    Args:
        task_name (str): O nome da tarefa.
        task_description (str): A descrição da tarefa.

    Returns:
        Callable: Uma função de tarefa configurada com o nome e a descrição fornecidos.
    """
    @task(task_id=task_name)
    def _task():
        _task.__doc__=f'{task_description}'
        return task_name

    return _task()

def create_databricks_task(task_id:str, job_id:int) -> DatabricksRunNowOperator:
    
    if Variable.get("env") == 'dev':
        is_test = True
    else:
        is_test = False        

    task = DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        notebook_params={
            "DATABRICKS_TOKEN": Variable.get("DATABRICKS_TOKEN"),
            "env":'databricks',
            'is_test':is_test
        },
        job_id=job_id
    )
    return task

def trigger_dag(dag_id: str) -> TriggerDagRunOperator:
    dag = TriggerDagRunOperator(
        task_id=f"trigger_{dag_id.lower()}_dag",
        trigger_dag_id=dag_id,
    )
    return dag

def task_group(task_id:str, catalogo_schema:str, path_select:str, path_exclude:str=None):
    os.environ["DATABRICKS_TOKEN"]: Variable.get("DATABRICKS_TOKEN")
    os.environ["env"] = Variable.get("env")
    afu = AirflowUtil()
    catalogos = {'gold':0, 'silver':1}
    path_exclude = f"path:models/{catalogo_schema.split('.')[0]}/BI_analytics/{path_exclude}" if not path_exclude else ""
    return afu.create_dbt_task_group(
        group_id=task_id,
        profile_name=f'{task_id}_profile',
        catalog=afu.get_catalogs()[catalogos[catalogo_schema.split('.')[0]]],
        schema=catalogo_schema.split('.')[1],
        select_paths=[
            f"path:models/{catalogo_schema.split('.')[0]}/BI_analytics/{path_select}"
        ],
        exclude=[
            path_exclude
        ],
    )

# Função para pegar a última execução da DAG
def get_last_execution_date(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    
    # Se houver execuções, ordenar pela data de execução
    if dag_runs:
        last_dag_run = max(dag_runs, key=lambda dr: dr.execution_date)
        print(last_dag_run.execution_date)
        return last_dag_run.execution_date
    return None

def dag_wait(dag_ids_to_wait):
    external_task_sensors = [
        ExternalTaskSensor(
            task_id=f'wait_for_{dag_id}',
            external_dag_id=dag_id,
            execution_date_fn=lambda execution_date, **kwargs: get_last_execution_date(dag_id),
            external_task_id=None,  # espera pelo DAG inteiro, não apenas uma task específica
            mode='reschedule',  # evita ocupar um worker enquanto espera
            timeout=3600,  # tempo limite de espera (em segundos)
            poke_interval=60  # intervalo entre checagens (em segundos)
        ) for dag_id in dag_ids_to_wait
    ]
    return (*external_task_sensors,)

def sleep_task(time_sleep:int):
    sleep_task = PythonOperator(
        task_id=f'sleep_{time_sleep}_minutes',
        python_callable=lambda: sleep(time_sleep*60),
    ) 
    return sleep_task