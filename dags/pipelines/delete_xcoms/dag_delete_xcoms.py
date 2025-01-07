from datetime import datetime
import os

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from config.data_contract_reader import CleanupContractReader
from include.teams_notifications import notify_teams

# Definindo a variável de ambiente
os.environ["env"] = Variable.get("env")

pipeline_options = {
    "env": "dev",
    "frequency": "weekly",
}

cleanup_data_contract = CleanupContractReader(
    frequency=pipeline_options["frequency"]
)

cleanup_params = cleanup_data_contract.get_cleanup_parameters()
JOBS_ID = cleanup_data_contract.get_databricks_job_id()

@dag(
    dag_id="Cleanup_Xcoms",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    schedule_interval=cleanup_data_contract.get_cron(),
    start_date=datetime(2024, 11, 1, 0, 0, 0),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=["weekly", "job_databricks", "xcom"],
)
def cleanup_xcoms_dag() -> None:
    """Define a DAG do Airflow para limpar entradas do XCom."""

    @task
    def start_task() -> str:
        """Task que indica o início do processo de limpeza."""
        return "Início da limpeza dos XComs"

    def cleanup_xcoms() -> DatabricksRunNowOperator:
        """Task para limpar os XComs antigos usando um job do Databricks."""
        cleanup_task = DatabricksRunNowOperator(
            task_id="cleanup_xcoms",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["cleanup_xcoms"],
            notebook_params={**cleanup_params, **pipeline_options},
        )
        return cleanup_task

    @task
    def end_task() -> str:
        """Task que indica o fim do processo de limpeza."""
        return "end_task"

    start = start_task()
    cleanup_xcoms = cleanup_xcoms()
    end = end_task()

    start >> cleanup_xcoms >> end

cleanup_xcoms_dag()
