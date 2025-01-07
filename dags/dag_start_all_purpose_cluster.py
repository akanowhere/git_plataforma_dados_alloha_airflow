from datetime import datetime

from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from include.teams_notifications import notify_teams


@dag(
    dag_id="start_all_purpose_cluster",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    schedule_interval="50 12-23,0-1 * * 1-6",
    start_date=datetime(
        datetime.today().year, datetime.today().month, datetime.today().day
    ),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=["job_databricks", "all_purpose", "start_databricks_cluster"],
)
def start_dag() -> None:
    """Define a DAG do Airflow para iniciar o cluster."""

    def start_all_purpose_cluster() -> DatabricksRunNowOperator:
        """Task para iniciar o cluster all purpose."""
        start_task = DatabricksRunNowOperator(
            task_id="start_all_purpose_cluster",
            databricks_conn_id="databricks_default",
            job_id=353787758266758,
        )
        return start_task

    start_task = start_all_purpose_cluster()

    start_task


start_dag()
