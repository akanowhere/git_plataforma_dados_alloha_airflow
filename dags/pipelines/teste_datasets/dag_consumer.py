from airflow.datasets import Dataset
from airflow.decorators import dag, task

from datetime import datetime


example_dataset = Dataset("s3://my-bucket/my-dataset/")


@dag(
    dag_id="test_consumer_dataset",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule=[example_dataset],
    start_date=datetime(2024, 10, 31),
    catchup=False,
    tags=["test", "datasets"],
)
def init():

    @task
    def print_hello():
        print("Hello, this DAG was triggered by a dataset update!")

    # Definindo a sequência de execução
    print_hello()


init()
