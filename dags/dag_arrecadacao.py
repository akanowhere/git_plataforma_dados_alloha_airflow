from datetime import timedelta

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Arrecadacao",
    default_args={
        "owner": "plataforma-dados-alloha",
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
    tags=["arrecadacao", "dbt", "silver", "seeds", "gold"],
)
def dag_arrecadacao() -> None:
    """
    DAG para executar processos relacionados a arrecadacao.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

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

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Criação do grupo de tarefas DBT para a gold
    gold_pdd_car = AirflowUtil().create_dbt_task_group(
        group_id="gold_pdd_car",
        profile_name="pdd_car",
        catalog=catalog_gold,
        schema="arrecadacao",
        select_paths=["path:models/gold/arrecadacao/pdd_car"],
        exclude=[""],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_pdd_tabelas_complementares = AirflowUtil().create_dbt_task_group(
        group_id="gold_pdd_tabelas_complementares",
        profile_name="pdd_tabelas_complementares",
        catalog=catalog_gold,
        schema="arrecadacao",
        select_paths=["path:models/gold/arrecadacao/pdd_tabelas_complementares"],
        exclude=[""],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_perfil_clientes_analitico = AirflowUtil().create_dbt_task_group(
        group_id="gold_perfil_clientes_analitico",
        profile_name="perfil_clientes_analitico",
        catalog=catalog_gold,
        schema="arrecadacao",
        select_paths=["path:models/gold/arrecadacao/perfil_clientes_analitico"],
        exclude=[""],
    )

    # Criação do grupo de tarefas DBT para a gold
    gold_perfil_clientes = AirflowUtil().create_dbt_task_group(
        group_id="gold_perfil_clientes",
        profile_name="perfil_clientes",
        catalog=catalog_gold,
        schema="arrecadacao",
        select_paths=["path:models/gold/arrecadacao/perfil_clientes"],
        exclude=[""],
    )

    gold_contratos_habilitados = AirflowUtil().create_dbt_task_group(
        group_id="gold_contratos_habilitados",
        profile_name="contratos_habilitados",
        catalog=catalog_gold,
        schema="arrecadacao",
        select_paths=["path:models/gold/arrecadacao/contratos_habilitados.sql"],
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
        >> wait_for_air_ingestion
        >> gold_pdd_car
        >> gold_pdd_tabelas_complementares
        >> gold_perfil_clientes_analitico
        >> gold_perfil_clientes
        >> gold_contratos_habilitados
        >> end_task
    )


dag_arrecadacao()
