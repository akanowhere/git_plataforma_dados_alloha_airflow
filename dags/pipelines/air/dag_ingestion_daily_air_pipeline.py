import ast
import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from config.data_contract_reader import DataContract
from include.teams_notifications import notify_teams

os.environ["env"] = Variable.get("env")

pipeline_options = {
    "incremental": "True",
    "frequency": "daily",
    "db_name": "air",
    "secrets_provider": "aws",
}

data_contract = DataContract(
    frequency=pipeline_options["frequency"], db_name=pipeline_options["db_name"]
)

landing_params = data_contract.get_landing_parameters()
bronze_params = data_contract.get_bronze_parameters()
triggered_dag_ids = ["Dims_base_chamados"]

schemas_dict_landing = ast.literal_eval(landing_params["schemas"])
schemas_dict_bronze = ast.literal_eval(bronze_params["schemas"])

JOBS_ID = data_contract.get_databricks_job_id()


@dag(
    dag_id="Ingestion_daily_air_pipeline",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=data_contract.get_cron(),
    start_date=days_ago(1),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=["air", "daily", "job_databricks", "landing", "bronze"],
)
def air_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    def air_to_landing(
        schema: str, table: str, table_params: dict
    ) -> DatabricksRunNowOperator:
        """Task para mover os dados do air para a camada landing."""
        landing_task = DatabricksRunNowOperator(
            task_id=f"{table}",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["landing"],
            notebook_params={
                **landing_params,
                **pipeline_options,
                "schemas": str({schema: {table: table_params}}),
            },
        )
        return landing_task

    def air_to_bronze(
        schema: str, table: str, table_params: dict
    ) -> DatabricksRunNowOperator:
        """Task para mover os dados da camada landing para a camada bronze."""
        bronze_task = DatabricksRunNowOperator(
            task_id=f"{table}",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["bronze"],
            notebook_params={
                **bronze_params,
                **pipeline_options,
                "schemas": str({schema: [{table: table_params}]}),
            },
        )
        return bronze_task

    def trigger_dags(dag_id: str) -> TriggerDagRunOperator:
        trigger_dag = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id.lower()}_dag",
            trigger_dag_id=dag_id,
        )
        return trigger_dag

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task_instance = start_task()
    end_task_instance = end_task()

    landing_groups = {}
    bronze_groups = {}

    for schema in schemas_dict_landing.keys():
        with TaskGroup(group_id=f"air_to_landing_{schema}") as landing_group:
            landing_tasks = []
            for table, table_params in schemas_dict_landing[schema].items():
                landing_task = air_to_landing(schema, table, table_params)
                landing_tasks.append(landing_task)

        landing_groups[schema] = landing_group

        with TaskGroup(group_id=f"air_to_bronze_{schema}") as bronze_group:
            bronze_tasks = []
            for table_dict in schemas_dict_bronze.get(schema, []):
                for table, table_params in table_dict.items():
                    # table_dict = [table]
                    bronze_task = air_to_bronze(schema, table, table_params)
                    bronze_tasks.append(bronze_task)

            bronze_groups[schema] = bronze_group

        # Conectar tarefas de landing com as de bronze
        for landing_task in landing_tasks:
            landing_task >> bronze_group

        # Conecta o grupo de landing e bronze com o start_task
        start_task_instance >> landing_group

    # Conectar todos os grupos de bronze e o end_task
    for bronze_group in bronze_groups.values():
        bronze_group >> end_task_instance

    # Trigger DAGs adicionais
    trigger_dag_operators = [trigger_dags(dag_id) for dag_id in triggered_dag_ids]

    # Conectar o end_task aos DAGs adicionais
    end_task_instance >> trigger_dag_operators


air_dag()
