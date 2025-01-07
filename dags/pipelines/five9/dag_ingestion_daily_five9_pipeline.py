from datetime import timedelta, datetime
from typing import Literal

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)
from airflow.utils.dates import days_ago
from config.data_contract_reader import ContractReader
from config.databricks_jobs_reader import DatabricksJobsReader
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil

# Setando variaveis do pipeline e obtendo as variaveis da UI do Airflow
pipeline_options = eval(Variable.get("FIVE9_PIPELINE_OPTIONS"))
pipeline_options["frequency"] = "daily"
env = Variable.get("env")

# Obtendo os Jobs ID do Databricks
databricks_jobs_id = DatabricksJobsReader(
    data_source=pipeline_options["data_source"],
    frequency=pipeline_options["frequency"],
    env=env,
).get_databricks_job_id()

# Obtendo os parâmetros das camadas landing e bronze
data_contract = ContractReader(
    frequency=pipeline_options["frequency"],
    source_type=pipeline_options["source_type"],
    data_source=pipeline_options["data_source"],
    env=env,
)
landing_params = data_contract.get_landing_parameters()
bronze_params = data_contract.get_bronze_parameters()

# Gerar schemas_info_landing como uma lista de dicionários, agrupando por schema_name
schemas_info_landing = [
    {schema_name: {table_name: about_ingestion}}
    for schema_name, tables_info in eval(landing_params["schemas"]).items()
    for table_name, about_ingestion in tables_info.items()
    if pipeline_options["frequency"] in about_ingestion["ingestion_freq"]
]

# Gerar schemas_info_bronze como uma lista de dicionários, agrupando por schema_name
schemas_info_bronze = [
    {schema_name: {table_name: about_ingestion}}
    for schema_name, tables_info in eval(bronze_params["schemas"]).items()
    for table_name, about_ingestion in tables_info.items()
    if pipeline_options["frequency"] in about_ingestion["ingestion_freq"]
]


def create_task_using_submit_operator(
    schema_info: dict,
    notebook_task: dict,
    suffix_task_id: Literal["to_landing", "to_bronze"],
):
    # Extrair o nome do schema e da tabela
    schema_name = list(schema_info.keys())[0]
    table_name = list(schema_info[schema_name].keys())[0]

    return DatabricksSubmitRunOperator(
        task_id=f"{table_name}_{pipeline_options['frequency']}_{suffix_task_id}",
        existing_cluster_id="0308-123840-h40p2at0",
        notebook_task=notebook_task,
        git_source={
            "git_url": "https://dev.azure.com/allohafibra/BI-ALLOHA/_git/plataforma-dados-alloha-databricks",
            "git_provider": "azureDevOpsServices",
            "git_branch": "dev",
        },
    )


# Define uma função para criar as tasks dinamicamente da camada landing
def create_task_using_run_now_operator(
    schema_info: dict,
    job_id: int,
    suffix_task_id: Literal["to_landing", "to_bronze"],
    notebook_params: dict,
) -> DatabricksRunNowOperator:
    # Extrair o nome do schema e da tabela
    schema_name = list(schema_info.keys())[0]
    table_name = list(schema_info[schema_name].keys())[0]

    return DatabricksRunNowOperator(
        task_id=f"{table_name}_{pipeline_options['frequency']}_{suffix_task_id}",
        databricks_conn_id="databricks_default",
        job_id=job_id,
        notebook_params=notebook_params,
    )


@dag(
    dag_id=f"Ingestion_{pipeline_options['frequency']}_{pipeline_options['data_source']}_pipeline",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=landing_params["cron"],
    start_date=datetime(2024,8,1,0,0,0),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=[
        pipeline_options["data_source"],
        pipeline_options["frequency"],
        "job_databricks",
        "landing",
        "bronze",
        "gold",
    ],
)
def five9_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    gold_catalog, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # @task_group(group_id="to_landing_tasks")
    # def to_landing_tasks_group():
    #     tasks = []
    #     for schema_info in schemas_info_landing:
    #         notebook_task = {
    #             "notebook_path": "data_platform_pipelines/pipelines/to_landing/api/ingestion_five9",
    #             "base_parameters": {
    #                 **landing_params,
    #                 **pipeline_options,
    #                 "schemas": str(schema_info),
    #             },
    #         }
    #         tasks.append(
    #             create_task_using_submit_operator(
    #                 schema_info, notebook_task, prefix_task_id="to_landing"
    #             )
    #         )
    @task_group(group_id="to_landing_tasks")
    def to_landing_tasks_group():
        tasks = []
        for schema_info in schemas_info_landing:
            notebook_params = {
                **landing_params,
                **pipeline_options,
                "schemas": str(schema_info),
            }
            tasks.append(
                create_task_using_run_now_operator(
                    schema_info=schema_info,
                    job_id=databricks_jobs_id["landing"],
                    suffix_task_id="to_landing",
                    notebook_params=notebook_params,
                )
            )

    # @task_group(group_id="to_bronze_tasks")
    # def to_bronze_tasks_group():
    #     tasks = []
    #     for schema_info in schemas_info_bronze:
    #         notebook_task = {
    #             "notebook_path": "data_platform_pipelines/pipelines/to_bronze/api/ingestion_five9",
    #             "base_parameters": {
    #                 **bronze_params,
    #                 **pipeline_options,
    #                 "schemas": str(schema_info),
    #             },
    #         }
    #         tasks.append(
    #             create_task_using_submit_operator(
    #                 schema_info, notebook_task, prefix_task_id="to_bronze"
    #             )
    #         )
    @task_group(group_id="to_bronze_tasks")
    def to_bronze_tasks_group():
        tasks = []
        for schema_info in schemas_info_bronze:
            notebook_params = {
                **bronze_params,
                **pipeline_options,
                "schemas": str(schema_info),
            }
            tasks.append(
                create_task_using_run_now_operator(
                    schema_info=schema_info,
                    job_id=databricks_jobs_id["bronze"],
                    suffix_task_id="to_bronze",
                    notebook_params=notebook_params,
                )
            )

    # Criação do grupo de tarefas dbt para a gold (fatos)
    to_gold_tasks = AirflowUtil().create_dbt_task_group(
        group_id="to_gold_tasks",
        profile_name="to_gold_tasks",
        catalog=gold_catalog,
        schema="five9",
        select_paths=["path:models/gold/five9"],
        exclude=["path:models/gold/five9/intermediate"],
        host=Variable.get("databricks_host_secret"),
        http_path=Variable.get("databricks_cluster_http_path_secret"),
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    to_landing = to_landing_tasks_group()
    to_bronze = to_bronze_tasks_group()
    to_gold = to_gold_tasks
    end_task = end_task()

    start_task >> to_landing >> to_bronze >> to_gold >> end_task


five9_dag()
