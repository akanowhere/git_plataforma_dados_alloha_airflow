from airflow.decorators import dag, task
from datetime import timedelta
from airflow.utils.dates import days_ago
import subprocess
import os

# Função que realiza a limpeza dos logs antigos
def clear_old_logs():
    log_path = "/opt/airflow/logs"  # Caminho dos logs
    days_to_keep_logs = 7  # Quantidade de dias de logs a manter
    days_to_keep_dags = 30  # Quantidade de dias de execuções de DAGs a manter

    # Comando para limpar os logs antigos (mais de 7 dias)
    command_logs = f"find {log_path} -type f -mtime +{days_to_keep_logs} -delete"
    # Comando para limpar as execuções de DAGs antigas (mais de 30 dias)
    command_dags = f"find {log_path}/ -type d -mtime +{days_to_keep_dags} -exec rm -rf {{}} +"

    try:
        subprocess.run(command_logs, shell=True, check=True)
        subprocess.run(command_dags, shell=True, check=True)
        print(f"Logs antigos (mais de {days_to_keep_logs} dias) e execuções de DAGs (mais de {days_to_keep_dags} dias) removidos com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao tentar limpar os logs e execuções de DAGs: {e}")
        raise

# Função para verificar o espaço em disco após a limpeza
def check_disk_space():
    command = "df -h /opt/airflow"
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Erro ao verificar o espaço em disco: {e}")
        raise

# Definindo a DAG com decoradores
@dag(
    dag_id="cleanup_logs_and_dags",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@daily",  # Agendado para rodar diariamente
    start_date=days_ago(1),  # Início imediato
    catchup=False,  # Não executar DAGs passadas
    tags=["maintenance", "cleanup", "logs", "dags"],
)
def cleanup_logs_and_dags_dag() -> None:
    """Define a DAG do Airflow para limpar logs antigos e execuções de DAGs."""

    @task
    def start_task() -> str:
        """Task que indica o início do processo de limpeza."""
        return "Início da limpeza dos logs e execuções de DAGs."

    @task
    def cleanup_logs_and_dags_task() -> str:
        """Task para executar a limpeza dos logs e execuções de DAGs."""
        clear_old_logs()
        return "Logs antigos e execuções de DAGs apagados com sucesso."

    @task
    def check_disk_space_task() -> str:
        """Task que verifica o espaço em disco após a limpeza."""
        space = check_disk_space()
        return f"Espaço em disco após a limpeza: \n{space}"

    @task
    def end_task() -> str:
        """Task que indica o fim do processo de limpeza."""
        return "Fim do processo de limpeza dos logs e execuções de DAGs."

    # Definindo a sequência de execução das tarefas
    start = start_task()
    cleanup = cleanup_logs_and_dags_task()
    disk_space = check_disk_space_task()
    end = end_task()

    start >> cleanup >> disk_space >> end

# Instancia a DAG
cleanup_logs_and_dags_dag()
