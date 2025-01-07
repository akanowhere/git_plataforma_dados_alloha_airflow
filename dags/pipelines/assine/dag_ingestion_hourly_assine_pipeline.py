import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from config.data_contract_reader import DataContract
from include.teams_notifications import notify_teams

os.environ["env"] = Variable.get("env")

pipeline_options = {
    "env": "prd",
    "incremental": "True",
    "frequency": "hourly",
    "db_name": "assine",
}

data_contract = DataContract(
    frequency=pipeline_options["frequency"], db_name=pipeline_options["db_name"]
)

landing_params = data_contract.get_landing_parameters()
bronze_params = data_contract.get_bronze_parameters()
JOBS_ID = data_contract.get_databricks_job_id()


@dag(
    dag_id="Ingestion_hourly_assine_pipeline",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=0.5),
    },
    schedule_interval=data_contract.get_cron(),
    start_date=days_ago(1),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=["assine", "hourly", "job_databricks", "landing", "bronze"],
)
def assine_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    def assine_to_landing() -> DatabricksRunNowOperator:
        """Task para mover os dados do assine para a camada landing."""
        landing_task = DatabricksRunNowOperator(
            task_id="assine_to_landing",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["landing"],
            notebook_params={**landing_params, **pipeline_options},
        )
        return landing_task

    def assine_to_bronze() -> DatabricksRunNowOperator:
        """Task para mover os dados da camada landing para a camada bronze."""
        bronze_task = DatabricksRunNowOperator(
            task_id="assine_to_bronze",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["bronze"],
            notebook_params={**bronze_params, **pipeline_options},
        )
        return bronze_task

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    to_landing = assine_to_landing()
    to_bronze = assine_to_bronze()
    end_task = end_task()

    start_task >> to_landing >> to_bronze >> end_task


assine_dag()
