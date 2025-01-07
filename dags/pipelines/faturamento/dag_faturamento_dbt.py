from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Faturamento",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval=None,
    start_date=days_ago(1),
    on_failure_callback=notify_teams,
    catchup=False,
    max_active_tasks=4,
    concurrency=5,
    tags=["faturamento", "dbt", "gold"],
)
def dag_faturamento() -> None:
    """
    DAG para executar processos relacionados ao faturamento 2.0.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Sensor externo para esperar pelo término da DAG air_ingestion
    wait_for_air_ingestion = ExternalTaskSensor(
        task_id="wait_for_air_ingestion",
        external_dag_id="Ingestion_daily_air_pipeline",
        external_task_id="end_task",  # Assumindo "end_task" como a última tarefa na DAG air_ingestion
        check_existence=True,
        timeout=14400,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
            dt, "Ingestion_daily_air_pipeline"
        ),
        mode="reschedule",
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_faturamento_fiscal = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_fiscal",
        profile_name="faturamento_fiscal",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/faturamento/fiscal"],
        exclude=[""],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_faturamento_auxiliar = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_auxiliar",
        profile_name="faturamento_auxiliar",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/auxiliar/faturamento_aux"],
        exclude=["path:models/gold/auxiliar/faturamento_aux/base_arvore_intermediate", "path:models/gold/auxiliar/faturamento_aux/faturamento_aux_suspensao.sql"],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_faturamento_aux_suspensao = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_aux_suspensao",
        profile_name="faturamento_aux_suspensao",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/auxiliar/faturamento_aux/faturamento_aux_suspensao.sql"],
        exclude=[""],
    )

    trigger_faturamento_base_analitca_complementares_dag = TriggerDagRunOperator(
        task_id="trigger_faturamento_base_analitca_complementares_dag",
        trigger_dag_id="Faturamento_base_analitica_complementares",
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
        >> wait_for_air_ingestion
        >> gold_faturamento_fiscal
        >> gold_faturamento_auxiliar
        >> gold_faturamento_aux_suspensao
        >> end_task
        >> trigger_faturamento_base_analitca_complementares_dag
    )


dag_faturamento()
