from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Pre_mailing",
    default_args={
        "owner": "BI_analytics",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None,
    start_date=days_ago(1),
    on_failure_callback=notify_teams,
    catchup=False,
    concurrency=5,
    tags=["mailing", "gold"],
)
def dag_pre_mailing() -> None:
    """
    DAG para executar processos relacionados ao mailing.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    # Sensor externo para esperar pelo término da DAG do Air daily
    wait_for_daily_air_pipeline = ExternalTaskSensor(
        task_id="wait_for_daily_air_pipeline",
        external_dag_id="Ingestion_daily_air_pipeline",
        external_task_id="end_task",  # Assumindo "end_task" como a última tarefa na DAG air_ingestion
        check_existence=True,
        timeout=14400,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: (dt - timedelta(days=1)).replace(
            hour=3, minute=10, second=0, microsecond=0
        ),
        mode="reschedule",
    )

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Criação do grupo de tarefas DBT para a gold
    dims_sydle = AirflowUtil().create_dbt_task_group(
        group_id="dims_sydle",
        profile_name="dims_sydle",
        catalog=catalog_gold,
        schema="sydle",
        select_paths=[
            "path:models/gold/sydle/dim_faturas_all_versions.sql",
            "path:models/gold/sydle/dim_negociacao.sql",
        ],
        exclude=[""],
    )

    fato_base = AirflowUtil().create_dbt_task_group(
        group_id="fato_base",
        profile_name="fato_base",
        catalog=catalog_gold,
        schema="base",
        select_paths=["path:models/gold/base/fato_cancelamento.sql"],
        exclude=[""],
    )

    dims_base = AirflowUtil().create_dbt_task_group(
        group_id="dims_base",
        profile_name="dims_base",
        catalog=catalog_gold,
        schema="base",
        select_paths=[
            "path:models/gold/base/dim_unidade.sql",
            "path:models/gold/base/dim_contrato.sql",
            "path:models/gold/base/dim_contrato_campanha.sql",
            "path:models/gold/base/dim_campanha.sql",
            "path:models/gold/base/dim_cliente.sql",
        ],
        exclude=[""],
    )

    dims_chamado = AirflowUtil().create_dbt_task_group(
        group_id="dims_chamado",
        profile_name="dims_chamado",
        catalog=catalog_gold,
        schema="chamados",
        select_paths=["path:models/gold/chamados/dim_chamado.sql"],
        exclude=[""],
    )

    trigger_mailing_cobranca_dag = TriggerDagRunOperator(
        task_id="trigger_mailing_cobranca_dag",
        trigger_dag_id="Mailing_cobranca",
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    # Definição do fluxo da DAG
    start_task = start_task()
    end_task = end_task()

    (
        start_task
        >> wait_for_daily_air_pipeline
        >> [dims_base, dims_chamado]
        >> fato_base
        >> trigger_mailing_cobranca_dag
        >> end_task
    )
    start_task >> dims_sydle >> trigger_mailing_cobranca_dag >> end_task


dag_pre_mailing()
