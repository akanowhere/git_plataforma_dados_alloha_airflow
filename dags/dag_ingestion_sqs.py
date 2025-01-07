import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable, BaseOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from config.data_contract_reader import Sqs
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil
from typing import Dict, List

os.environ["env"] = Variable.get("env")
sqs_contract = Sqs()

QUEUES = sqs_contract.get_queues()
JOBS_ID = sqs_contract.get_databricks_job_id()
triggered_dag_ids = ["Pre_mailing"]


@dag(
    dag_id="Ingestion_sydle_pipeline",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=0.5),
    },
    schedule_interval=sqs_contract.get_cron(),
    start_date=days_ago(1),
    on_failure_callback=notify_teams,
    catchup=False,
    max_active_tasks=7,
    tags=["sqs", "daily", "job_databricks", "landing", "bronze", "silver", "gold"],
)
def sydle_dag() -> None:
    """Define a DAG do Airflow para o pipeline de ingestão."""

    catalog_gold, catalog_silver = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    def sydle_to_landing(table: str) -> DatabricksRunNowOperator:
        """Task para mover os dados do sqs para a camada landing."""
        landing_task = DatabricksRunNowOperator(
            task_id=f"sydle_to_landing_{table}",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["landing"],
            notebook_params=sqs_contract.get_landing_params(table),
        )
        return landing_task

    def sydle_to_bronze(table: str) -> DatabricksRunNowOperator:
        """Task para mover os dados do sqs para a camada bronze."""
        bronze_task = DatabricksRunNowOperator(
            task_id=f"sydle_to_bronze_{table}",
            databricks_conn_id="databricks_default",
            job_id=JOBS_ID["bronze"],
            notebook_params=sqs_contract.get_bronze_params(table),
        )
        return bronze_task

    # sydle_stage = AirflowUtil().create_dbt_task_group(
    #     group_id="stage_sydle",
    #     profile_name="stage_sydle",
    #     catalog=catalog_silver,
    #     schema="stage",
    #     select_paths=["path:models/silver/stage/vw_sydle_tbl_cliente.sql",
    #                   "path:models/silver/stage/vw_sydle_tbl_negociacao.sql",
    #                   "path:models/silver/stage/vw_sydle_tbl_venda_qualify_vendedor.sql"],
    #     exclude=[""],
    # )

    # sydle_to_silver = AirflowUtil().create_dbt_task_group(
    #     group_id="silver_sydle",
    #     profile_name="silver_sydle",
    #     catalog=catalog_silver,
    #     schema="stage_sydle",
    #     select_paths=["path:models/silver/sydle"],
    #     exclude=[""],
    # )

    # Sensor externo para esperar pelo término da DAG dim (dim_faturas_mailing depende de 3 dims)
    # wait_for_dim_ingestion = ExternalTaskSensor(
    #     task_id="wait_for_dim_ingestion",
    #     external_dag_id="Dims_base_chamados",
    #     external_task_group_id="dims_base",
    #     check_existence=True,
    #     timeout=14400,
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"],
    #     execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
    #         dt, "Dims_base_chamados", manually_triggered=True
    #     ),
    #     mode="reschedule",
    # )

    # sydle_to_gold = AirflowUtil().create_dbt_task_group(
    #     group_id="gold_sydle",
    #     profile_name="gold_sydle",
    #     catalog=catalog_gold,
    #     schema="sydle",
    #     select_paths=[
    #         "path:models/gold/sydle/dim_faturas_all_versions.sql",
    #         "path:models/gold/sydle/dim_negociacao.sql"
    #     ],
    #     exclude=[""],
    # )

    def sydle_trigger_dag(dag_id: str) -> TriggerDagRunOperator:
        trigger_dag = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id.lower()}_dag",
            trigger_dag_id=dag_id,
        )
        return trigger_dag

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()

    to_landing = {queue: sydle_to_landing(queue) for queue in QUEUES}
    to_bronze = {queue: sydle_to_bronze(queue) for queue in QUEUES}

    end_task = end_task()

    trigger_dag = {dag_id: sydle_trigger_dag(dag_id) for dag_id in triggered_dag_ids}

    def create_tasks(
        queues: List[str],
        to_landing: Dict[str, BaseOperator],
        to_bronze: Dict[str, BaseOperator],
        start_task: BaseOperator,
        end_task: BaseOperator,
        trigger_dag: Dict[str, BaseOperator],
        triggered_dag_ids: List[str]
    ) -> None:
        """
        Cria e conecta as tasks do Airflow para as filas especificadas,
        incluindo tarefas de landing, bronze, e triggers para DAGs

        Args:
            queues (List[str]): Lista de nomes das filas a serem processadas.
            to_landing (Dict[str, BaseOperator]): Dicionário que mapeia nomes de filas para tasks de landing.
            to_bronze (Dict[str, BaseOperator]): Dicionário que mapeia nomes de filas para tasks de bronze.
            start_task (BaseOperator): Task inicial que inicia o fluxo de execução.
            end_task (BaseOperator): Task final que encerra o fluxo de execução.
            trigger_dag (Dict[str, BaseOperator]): Dicionário que mapeia IDs de DAGs para tasks de trigger.
            triggered_dag_ids (List[str]): Lista de IDs de DAGs que devem ser acionados.
        """
        # Conecta as tasks de landing e bronze
        for queue in queues:
            to_landing_task = to_landing[queue]
            to_bronze_task = to_bronze[queue]
            start_task >> to_landing_task >> to_bronze_task

        # Define as mailing tasks e os triggers
        mailing_tasks = [to_bronze["negociacao"], to_bronze["fatura"]]
        trigger_tasks = [trigger_dag[dag_id] for dag_id in triggered_dag_ids]

        # Conecta as mailing tasks aos triggers
        for mailing_task in mailing_tasks:
            mailing_task >> trigger_tasks

        # Conecta todas as tasks de bronze ao end_task, exceto as mailing_tasks
        non_mailing_bronze_tasks = [
            to_bronze[queue] for queue in queues if queue not in ["negociacao", "fatura"]
        ]
        end_task.set_upstream(non_mailing_bronze_tasks)

        # Conecta os triggers ao end_task
        trigger_tasks >> end_task

    create_tasks(
        QUEUES, to_landing, to_bronze,
        start_task, end_task, trigger_dag, triggered_dag_ids
    )


sydle_dag()
