from datetime import datetime

from airflow.decorators import dag, task
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="carga_seeds",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    schedule_interval=None,
    start_date=datetime(
        datetime.today().year, datetime.today().month, datetime.today().day
    ),
    on_failure_callback=notify_teams,
    catchup=False,
    tags=["seeds"],
)
def dag_carga_seeds() -> None:
    """
    DAG para executar as seeds do dbt.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"

    # Descomente esta linha para criar o grupo de tarefas para silver (por default são views)
    silver_subregionais = AirflowUtil().create_dbt_task_group(
        group_id="silver_subregionais",
        profile_name="silver_carga_seeds",
        catalog="silver",
        schema="stage_seeds_data",
        select_paths=["path:seeds/silver"],
        exclude=[""],
    )

    # gold_venda_hora = AirflowUtil().create_dbt_task_group(
    #     group_id="gold_venda_hora",
    #     profile_name="gold_carga_seeds",
    #     catalog=catalog_gold,
    #     schema="venda_hora",
    #     select_paths=["path:seeds/gold/venda_hora"],
    #     exclude=[""],
    # )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    end_task = end_task()

    start_task >> silver_subregionais >> end_task


dag_carga_seeds()
