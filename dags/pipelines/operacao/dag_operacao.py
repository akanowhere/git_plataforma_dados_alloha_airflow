from datetime import timedelta

from airflow.decorators import dag, task
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Operacao",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None,
    on_failure_callback=notify_teams,
    catchup=False,
    max_active_tasks=4,
    concurrency=5,
    tags=["operacao", "dbt", "gold"],
)
def dag_operacao() -> None:
    """
    DAG para executar o pipeline de operacao.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Criação do grupo de tarefas DBT para a gold
    operacao = AirflowUtil().create_dbt_task_group(
        group_id="operacao",
        profile_name="operacao",
        catalog=catalog_gold,
        schema="operacao",
        select_paths=["path:models/gold/operacao"],
        exclude=[""],
    )

    analiticos = AirflowUtil().create_dbt_task_group(
        group_id="operacao_analiticos",
        profile_name="operacao",
        catalog=catalog_gold,
        schema="operacao",
        select_paths=["path:models/gold/BI_analytics/EPS/analiticos"],
        exclude=[""],
    )

    indicadores = AirflowUtil().create_dbt_task_group(
        group_id="operacao_indicadores",
        profile_name="operacao",
        catalog=catalog_gold,
        schema="operacao",
        select_paths=["path:models/gold/BI_analytics/EPS/indicadores"],
        exclude=[""],
    )

    bonificacao = AirflowUtil().create_dbt_task_group(
        group_id="operacao_bonificacao",
        profile_name="operacao",
        catalog=catalog_gold,
        schema="operacao",
        select_paths=["path:models/gold/BI_analytics/EPS/bonificacao"],
        exclude=[""],
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    end_task = end_task()

    start_task >> operacao >> analiticos >> indicadores >> bonificacao >> end_task


dag_operacao()
