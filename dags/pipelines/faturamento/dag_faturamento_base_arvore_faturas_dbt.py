from datetime import timedelta

from airflow.decorators import dag, task
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Faturamento_base_arvore_faturas",
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
    tags=["faturamento_base", "dbt", "gold"],
)
def dag_faturamento_base() -> None:
    """
    DAG para executar processos relacionados ao faturamento 2.0.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Criação do grupo de tarefas DBT para a gold
    gold_faturamento_base_arvore_faturas = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_base_arvore_faturas",
        profile_name="faturamento_base_arvore_faturas",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/faturamento/base_arvore_faturas/base_arvore_faturas.sql"],
        exclude=[""],
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
        >> gold_faturamento_base_arvore_faturas
        >> end_task
    )


dag_faturamento_base()
