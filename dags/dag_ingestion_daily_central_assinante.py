from datetime import timedelta

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from config.data_contract_reader import central_assinante_ContractReader
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil

# Setando variaveis do pipeline e obtendo as variaveis da UI do Airflow
pipeline_options = {
    "incremental": "True",
    "frequency": "daily",
    "source_type": "firebase",
    "data_source": "central_assinante",
}
pipeline_options["APP_AUTH_CENTRAL_ASSINANTE"] = Variable.get("APP_AUTH_CENTRAL_ASSINANTE")

# Obtendo os parâmetros das camadas landing e bronze
data_contract = central_assinante_ContractReader(
    frequency=pipeline_options["frequency"],
    source_type=pipeline_options["source_type"],
    data_source=pipeline_options["data_source"],
    env=Variable.get("env"),
)
landing_params = data_contract.get_landing_parameters()
bronze_params = data_contract.get_bronze_parameters()
JOBS_ID = data_contract.get_databricks_job_id()

# Gerar schemas_info_landing como uma lista de dicionários, agrupando por schema_name
schemas_info_landing = [
    {schema_name: tables_info}
    for schema_name, tables_info in eval(landing_params["schemas"]).items()
]

# Gerar schemas_info_bronze como uma lista de dicionários, agrupando por schema_name
schemas_info_bronze = [
    {schema_name: tables_info}
    for schema_name, tables_info in eval(bronze_params["schemas"]).items()
]


# Define uma função para criar as tasks dinamicamente da camada landing
def create_to_landing_task(schema_info: dict) -> DatabricksRunNowOperator:
    # Extrair o nome do schema
    schema_name = list(schema_info.keys())[0]

    return DatabricksRunNowOperator(
        task_id=f"{schema_name}_{pipeline_options['frequency']}_to_landing",
        databricks_conn_id="databricks_default",
        job_id=JOBS_ID["landing"],
        notebook_params={
            **landing_params,
            **pipeline_options,
            "schemas": str(schema_info),
        },
    )


def create_to_bronze_task(schema_info: dict) -> DatabricksRunNowOperator:
    # Extrair o nome do schema
    schema_name = list(schema_info.keys())[0]

    return DatabricksRunNowOperator(
        task_id=f"{schema_name}_{pipeline_options['frequency']}_to_bronze",
        databricks_conn_id="databricks_default",
        job_id=JOBS_ID["bronze"],
        notebook_params={
            **bronze_params,
            **pipeline_options,
            "schemas": str(schema_info),
        },
    )


@dag(
    dag_id="Ingestion_daily_central_assinante_pipeline",
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
    tags=["central_assinante", "daily", "job_databricks", "landing", "bronze"],
)
def central_assinante_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    gold_catalog, silver_catalog = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    @task_group(group_id="to_landing_tasks")
    def to_landing_tasks_group():
        tasks = []
        for schema_info in schemas_info_landing:
            tasks.append(create_to_landing_task(schema_info))

    @task_group(group_id="to_bronze_tasks")
    def to_bronze_tasks_group():
        tasks = []
        for schema_info in schemas_info_bronze:
            tasks.append(create_to_bronze_task(schema_info))

    """
        to_silver_tasks = AirflowUtil().create_dbt_task_group(
        group_id="to_silver_tasks",
        profile_name="to_silver_tasks",
        catalog=silver_catalog,
        schema="app",
        select_paths=["path:models/silver/stage/app"],
        exclude=[""],
    )"""

    to_gold_tasks = AirflowUtil().create_dbt_task_group(
        group_id="to_gold_tasks",
        profile_name="to_gold_tasks",
        catalog=gold_catalog,
        schema="central_assinante",
        select_paths=["path:models/gold/central_assinante"],
        exclude=[""],
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    (
        start_task()
        >> to_landing_tasks_group()
        >> to_bronze_tasks_group()
        >> to_gold_tasks
        >> end_task()
    )


central_assinante_dag()
