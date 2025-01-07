from datetime import timedelta

from airflow.decorators import dag, task
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil
from airflow.operators.dagrun_operator import TriggerDagRunOperator


@dag(
    dag_id="Faturamento_base_arvore_faturas_intermediate",
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
    gold_faturamento_intermediate_base_arvore = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_intermediate_base_arvore",
        profile_name="faturamento_intermediate_base_arvore",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/auxiliar/faturamento_aux/base_arvore_intermediate/base_arvore_inicial"],
        exclude=[""],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_faturamento_intermediate_tag_isencoes_e_tag = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_intermediate_tag_isencoes_e_tag",
        profile_name="faturamento_intermediate_tag_isencoes_e_tag",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/auxiliar/faturamento_aux/base_arvore_intermediate/tag_isencoes_e_tag"],
        exclude=[""],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_faturamento_base_faturas_meses_passados = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_base_faturas_meses_passados",
        profile_name="faturamento_base_faturas_meses_passados",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/faturamento/base_faturas_meses_passados"],
        exclude=[""],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_faturamento_intermediate_tags_tela_3 = AirflowUtil().create_dbt_task_group(
        group_id="gold_faturamento_intermediate_tags_tela_3",
        profile_name="faturamento_intermediate_tags_tela_3",
        catalog=catalog_gold,
        schema="faturamento",
        select_paths=["path:models/gold/auxiliar/faturamento_aux/base_arvore_intermediate/tag_tela_3"],
        exclude=[""],
    )

    trigger_faturamento_base_arvore_faturas_dag = TriggerDagRunOperator(
        task_id="trigger_faturamento_base_arvore_faturas_dag",
        trigger_dag_id="Faturamento_base_arvore_faturas",
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
        >> gold_faturamento_intermediate_base_arvore
        >> gold_faturamento_intermediate_tag_isencoes_e_tag
        >> gold_faturamento_base_faturas_meses_passados
        >> gold_faturamento_intermediate_tags_tela_3
        >> end_task
        >> trigger_faturamento_base_arvore_faturas_dag
    )


dag_faturamento_base()
