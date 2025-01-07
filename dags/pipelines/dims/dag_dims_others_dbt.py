from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from include.teams_notifications import notify_teams
from utils.airflow_utils import AirflowUtil


@dag(
    dag_id="Dims_others",
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
    tags=["dims", "contato", "venda", "dbt", "gold", "tv", "telefonia"],
)
def dag_dims_others() -> None:
    """
    DAG para executar as dimensões.
    """
    catalog_gold, _ = AirflowUtil().get_catalogs()

    @task
    def start_task() -> str:
        """Task que indica o início do pipeline."""
        return "start_task"


    # Sensor externo para esperar pelo término da DAG do Assine
    wait_for_assine_ingestion = ExternalTaskSensor(
        task_id="wait_for_assine_ingestion",
        external_dag_id="Ingestion_daily_assine_pipeline",
        external_task_id="end_task",
        check_existence=True,
        timeout=14400,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=lambda dt: AirflowUtil.get_most_recent_dag_run(
            dt, "Ingestion_daily_assine_pipeline"
        ),
        mode="reschedule",
    )

    dims_contato = AirflowUtil().create_dbt_task_group(
        group_id="dims_contato",
        profile_name="dims_contato",
        catalog=catalog_gold,
        schema="contato",
        select_paths=["path:models/gold/contato"],
        exclude=[""],
    )

    dims_venda = AirflowUtil().create_dbt_task_group(
        group_id="dims_venda",
        profile_name="dims_venda",
        catalog=catalog_gold,
        schema="venda",
        select_paths=["path:models/gold/venda"],
        exclude=["path:models/gold/venda/fato_venda.sql"],
    )

    dims_tv = AirflowUtil().create_dbt_task_group(
        group_id="tv",
        profile_name="tv",
        catalog=catalog_gold,
        schema="tv",
        select_paths=["path:models/gold/tv"],
        exclude=[""],
    )

    dim_telefonia = AirflowUtil().create_dbt_task_group(
        group_id="telefonia",
        profile_name="telefonia",
        catalog=catalog_gold,
        schema="venda",
        select_paths=["path:models/gold/telefonia"],
        exclude=[""],
    )

    trigger_fatos_dag = TriggerDagRunOperator(
        task_id="trigger_fatos_dag",
        trigger_dag_id="Fatos",
    )

    @task
    def end_task() -> str:
        """Task que indica o fim do pipeline."""
        return "end_task"

    start_task = start_task()
    end_task = end_task()

    start_task >> [dims_venda, dims_tv, dim_telefonia] >> end_task
    start_task >> wait_for_assine_ingestion >> dims_contato >> end_task
    dims_venda >> end_task
    end_task >> trigger_fatos_dag


dag_dims_others()
