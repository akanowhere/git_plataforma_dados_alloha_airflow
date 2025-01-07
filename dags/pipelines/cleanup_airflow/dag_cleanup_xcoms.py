from datetime import datetime, timedelta
import pytz
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import XCom
from airflow.utils.session import provide_session
from include.teams_notifications import notify_teams

# Função para limpar os XComs com mais de 7 dias
@provide_session
def clear_old_xcoms(session=None):
    """Remove XComs antigos com mais de 7 dias."""
    # Usando UTC para garantir que a data tenha fuso horário
    seven_days_ago = datetime.now(pytz.utc) - timedelta(days=2)

    # Garantindo que a data 'seven_days_ago' seja do tipo 'timestamp with time zone'
    seven_days_ago = seven_days_ago.astimezone(pytz.utc)

    session.query(XCom).filter(XCom.execution_date < seven_days_ago).delete(synchronize_session=False)
    session.commit()

# Definindo a DAG com decoradores
@dag(
    dag_id="cleanup_xcoms",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval="@daily",  # Intervalo para rodar diariamente
    start_date=days_ago(1),  # Iniciar imediatamente
    catchup=False,  # Não executar DAGs passadas
    on_failure_callback=notify_teams,
    tags=["maintenance", "cleanup", "xcom"],
)
def cleanup_xcoms_dag() -> None:
    """Define a DAG do Airflow para limpar entradas do XCom."""

    @task
    def start_task() -> str:
        """Task que indica o início do processo de limpeza."""
        return "Início da limpeza dos XComs"

    @task
    def cleanup_xcoms_task() -> str:
        """Task para limpar os XComs antigos."""
        clear_old_xcoms()
        return "XComs antigos apagados."

    @task
    def end_task() -> str:
        """Task que indica o fim do processo de limpeza."""
        return "Fim do processo de limpeza de XComs."

    # Definindo a sequência de execução das tarefas
    start = start_task()
    cleanup = cleanup_xcoms_task()
    end = end_task()

    start >> cleanup >> end

# Instancia a DAG
cleanup_xcoms_dag()
