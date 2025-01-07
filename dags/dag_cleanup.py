from datetime import datetime
import os

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import \
    DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from config.data_contract_reader import CleanupContractReader
from include.teams_notifications import notify_teams

os.environ["env"] = Variable.get("env")

pipeline_options = {
    "frequency": "weekly",
}

cleanup_data_contract = CleanupContractReader(
    frequency=pipeline_options["frequency"]
)

cleanup_params = cleanup_data_contract.get_cleanup_parameters()
job_id = cleanup_data_contract.get_databricks_job_id()

@dag(
    dag_id="Cleanup",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    schedule_interval=cleanup_data_contract.get_cron(),
    start_date=datetime(2024,11,1,0,0,0),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=["weekly", "job_databricks", "bronze"],
)
def cleanup_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    def cleanup() -> DatabricksRunNowOperator:
        """Task para mover os dados da camada landing para a camada bronze."""
        bronze_task = DatabricksRunNowOperator(
            task_id="cleanup",
            databricks_conn_id="databricks_default",
            job_id=job_id["cleanup"],
            notebook_params={**cleanup_params, **pipeline_options},
        )
        return bronze_task
    
    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    cleanup_task = cleanup()
    end_task = end_task()

    start_task >> cleanup_task >> end_task 


cleanup_dag()
