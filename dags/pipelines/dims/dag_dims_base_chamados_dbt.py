from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Dims_base_chamados",
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
    tags=["dims", "base", "chamados"],
)
def dag_dims_base_chamados() -> None:
    """
    DAG para executar as dimensões.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Criação do grupo de tarefas DBT para a gold
    dims_base = AirflowUtil().create_dbt_task_group(
        group_id="dims_base",
        profile_name="dims_base",
        catalog=catalog_gold,
        schema="base",
        select_paths=["path:models/gold/base"],
        exclude=[
            "path:models/gold/base/fato_ativacao.sql",
            "path:models/gold/base/fato_base_ativos.sql",
            "path:models/gold/base/fato_cancelamento.sql",
            "path:models/gold/base/fato_primeira_ativacao.sql",
            "path:models/gold/base/fato_base_ativos_analitica.sql",
            # As golds a seguir executam na dag pre-mailing
            "path:models/gold/base/dim_unidade.sql",
            "path:models/gold/base/dim_contrato.sql",
            "path:models/gold/base/dim_contrato_campanha.sql",
            "path:models/gold/base/dim_campanha.sql",
            "path:models/gold/base/dim_cliente.sql",
        ],
    )

    dims_chamados = AirflowUtil().create_dbt_task_group(
        group_id="dims_chamados",
        profile_name="dims_chamados",
        catalog=catalog_gold,
        schema="chamados",
        select_paths=["path:models/gold/chamados"],
        exclude=[
            "path:models/gold/chamados/dim_chamado.sql"
        ],  # Dim executa na dag pre-mailing
    )

    trigger_dims_others = TriggerDagRunOperator(
        task_id="trigger_dims_others",
        trigger_dag_id="Dims_others",
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    end_task = end_task()

    start_task >> [dims_base, dims_chamados] >> end_task >> trigger_dims_others


dag_dims_base_chamados()
