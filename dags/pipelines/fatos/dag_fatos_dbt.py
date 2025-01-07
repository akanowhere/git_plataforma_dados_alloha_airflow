from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil
from airflow.utils.trigger_rule import TriggerRule

triggered_dag_ids = [
    "Relatorios",
    "Fpd_spd",
    "Fpd",
    "Faturamento",
    "Arrecadacao",
    "Cobranca",
    "Operacao",
]


@dag(
    dag_id="Fatos",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval=None,
    on_failure_callback=notify_teams,
    catchup=False,
    max_active_tasks=4,
    concurrency=5,
    tags=["fatos", "base", "venda", "dbt", "gold"],
)
def dag_fatos() -> None:
    """
    DAG para executar as fatos.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    fatos_base = AirflowUtil().create_dbt_task_group(
        group_id="fatos_base",
        profile_name="fatos_base",
        catalog=catalog_gold,
        schema="base",
        select_paths=["path:models/gold/base/fato_*"],
        exclude=["path:models/gold/base/fato_cancelamento.sql"], # Roda no processo pre-mailing
    )

    fatos_venda = AirflowUtil().create_dbt_task_group(
        group_id="fatos_venda",
        profile_name="fatos_venda",
        catalog=catalog_gold,
        schema="venda",
        select_paths=["path:models/gold/venda/fato_venda.sql"],
        exclude=[""],
    )

    # Sensor externo para esperar pelo término da DAG dim (dim_faturas_mailing depende de 3 dims)
    wait_for_sydle_ingestion = ExternalTaskSensor(
        task_id="wait_for_sydle_ingestion",
        external_dag_id="Ingestion_sydle_pipeline",
        external_task_id="end_task",
        check_existence=True,
        timeout=25200,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: (dt - timedelta(days=1)).replace(
            hour=3, minute=10, second=0, microsecond=0
        ),
        mode="reschedule",
    )

    sydle_to_gold = AirflowUtil().create_dbt_task_group(
        group_id="gold_sydle",
        profile_name="gold_sydle",
        catalog=catalog_gold,
        schema="sydle",
        select_paths=["path:models/gold/sydle"],
        exclude=[
            "path:models/gold/sydle/dim_faturas_all_versions.sql",
            "path:models/gold/sydle/dim_negociacao.sql"
        ],
    )

    def fatos_trigger_dag(dag_id: str) -> TriggerDagRunOperator:
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
    end_task = end_task()

    trigger_dag = {dag_id: fatos_trigger_dag(dag_id) for dag_id in triggered_dag_ids}

    start_task >> [fatos_base, fatos_venda] >> end_task
    start_task >> wait_for_sydle_ingestion >> sydle_to_gold >> end_task

    # Disparo condicional para Mailing_cobranca após fatos_base e gold_sydle
    # [fatos_base, sydle_to_gold] >> trigger_dag["Mailing_cobranca"]

    # # end_task será executado após Mailing_cobranca, independentemente do sucesso ou falha
    # trigger_dag["Mailing_cobranca"].trigger_rule = TriggerRule.ALL_DONE
    # trigger_dag["Mailing_cobranca"] >> end_task

    # Outras DAGs serão disparadas após o end_task
    end_task >> [trigger_dag[dag_id] for dag_id in triggered_dag_ids]


dag_fatos()
