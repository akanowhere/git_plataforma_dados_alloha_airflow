import os
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from config.data_contract_reader import DataContract
from include.teams_notifications import notify_teams

os.environ["env"] = Variable.get("env")

pipeline_options = {
    "incremental": "True",
    "frequency": "daily",
    "db_name": "db_financeiro",
    "secrets_provider": "aws",
}

data_contract = DataContract(
    frequency=pipeline_options["frequency"], db_name=pipeline_options["db_name"]
)

landing_params = data_contract.get_landing_parameters()
bronze_params = data_contract.get_bronze_parameters()

JOBS_ID = data_contract.get_databricks_job_id()


@dag(
    dag_id="Ingestion_daily_db_financeiro",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval="10 3 * * 1-5",
    start_date=datetime(2024,8,1,0,0,0),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=[
        "db_financeiro",
        "faturamento",
        "daily",
        "job_databricks",
        "landing",
        "bronze",
    ],
)
def db_financeiro_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    def db_financeiro_to_landing() -> DatabricksRunNowOperator:
        """Task para mover os dados do db_financeiro para a camada landing."""
        landing_task = DatabricksRunNowOperator(
            task_id="db_financeiro_to_landing",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["landing"],
            notebook_params={**landing_params, **pipeline_options},
        )
        return landing_task

    def db_financeiro_to_bronze() -> DatabricksRunNowOperator:
        """Task para mover os dados da camada landing para a camada bronze."""
        bronze_task = DatabricksRunNowOperator(
            task_id="db_financeiro_to_bronze",
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
    to_landing = db_financeiro_to_landing()
    to_bronze = db_financeiro_to_bronze()
    end_task = end_task()

    start_task >> to_landing >> to_bronze >> end_task


db_financeiro_dag()
