import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from config.data_contract_reader import DataContract
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil
from utils.bi_utils import (
    standard_task,
    DEFAULT_ARGS_DAGS,
    create_databricks_task
)

# Configuração de variáveis de ambiente com valores sensíveis
os.environ["DATABRICKS_TOKEN"] = Variable.get("DATABRICKS_TOKEN")
os.environ["env"] = Variable.get("env")
JOBS_ID = DataContract().get_databricks_job_id("venda_hora")
DEFAULT_ARGS_DAGS["default_args"]["owner"] = "plataforma-dados-alloha"

@dag(
    dag_id="venda_hora",
    **DEFAULT_ARGS_DAGS,
    schedule_interval="02 12-23,0-1 * * 1-6",
    tags=["venda_hora", "dbt", "silver", "seeds", "gold"],
)

def dag_venda_hora() -> None:
    """
    DAG para executar processos relacionados à venda por hora.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    # Sensor externo para esperar pelo término da DAG air_ingestion
    wait_for_air_ingestion = ExternalTaskSensor(
        task_id="wait_for_air_ingestion",
        external_dag_id="Ingestion_hourly_air_pipeline",
        external_task_id="end_task",  # Assumindo "end_task" como a última tarefa na DAG air_ingestion
        check_existence=True,
        timeout=7200,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: dt - timedelta(minutes=2),
        mode="reschedule",
    )

    # Sensor externo para esperar pelo término da DAG assine_ingestion
    wait_for_assine_ingestion = ExternalTaskSensor(
        task_id="wait_for_assine_ingestion",
        external_dag_id="Ingestion_hourly_assine_pipeline",
        external_task_id="end_task",  # Assumindo "end_task" como a última tarefa na DAG assine_ingestion
        timeout=7200,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: dt - timedelta(minutes=2),
        mode="reschedule",
    )

    # Criação do grupo de tarefas DBT para a gold
    gold = AirflowUtil().create_dbt_task_group(
        group_id="gold",
        profile_name="venda_hora_gold",
        catalog=catalog_gold,
        schema="venda_hora",
        select_paths=["path:models/gold/venda_hora"],
        exclude=[""],
        host=Variable.get("databricks_host_secret"),
        http_path=Variable.get("databricks_hourly_cluster_http_path_secret"),
    )

    (
        standard_task("start_task", "Task que indica o início do pipeline.") 
        >> [wait_for_assine_ingestion, wait_for_air_ingestion]
        >> gold
        >> create_databricks_task('Envio_do_email', JOBS_ID["venda_hora"])
        >> standard_task("end_task", "Task que indica o fim do pipeline.") 
    )


dag_venda_hora()
