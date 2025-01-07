from airflow.datasets import Dataset
from airflow.decorators import dag, task


example_dataset = Dataset("s3://my-bucket/my-dataset/")


@dag(
    dag_id="test_producer_dataset",
    default_args={
        "owner": "plataforma-dados-alloha",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    schedule_interval=None,
    catchup=False,
    tags=["test", "datasets"],
)
def init():

    @task(outlets=[example_dataset])
    def produce_dataset():
        print("Producing dataset...")

    # Definindo a sequência de execução
    produce_dataset()


init()
