from typing import Literal

import yaml


class DatabricksJobsReader:
    """
    Classe base para ler contratos de dados a partir de arquivos YAML.

    Args:
        env (str): O ambiente configurado na variável de ambiente 'env'.
        data_source (str): O nome da fonte de dados.
        yml_file (str, opcional): O caminho para o arquivo YAML contendo os IDs dos jobs do databricks. O padrão é '/opt/airflow/dags/config/databricks_jobs.yaml'.
    """

    def __init__(
        self,
        data_source: str,
        frequency: Literal["hourly", "daily"] = None,
        env: Literal["dev", "prd"] = "dev",
        yml_file: str = "/opt/airflow/dags/config/databricks_jobs.yaml",
    ):
        """Inicializa o leitor do id de jobs do databricks com base na fonte de dados e ambiente."""
        self.data_source = data_source
        self.frequency = frequency
        self.env = env
        self.yml_file = yml_file
        self.databricks_jobs = (
            self._get_docs()[0][self.data_source][self.frequency]
            if self.frequency
            else self._get_docs()[0][self.data_source]
        )

    def _get_docs(self) -> list[dict]:
        """
        Carrega e retorna os documentos YAML do arquivo especificado.

        Returns:
            list[dict]: Uma lista contendo os documentos YAML carregados.
        """
        data_contract = []
        with open(self.yml_file, "r") as file:
            for doc in yaml.safe_load_all(file):
                data_contract.append(doc)

        return data_contract

    def get_databricks_job_id(self) -> dict:
        return {
            "landing": self.databricks_jobs["landing"][self.env],
            "bronze": self.databricks_jobs["bronze"][self.env],
        }
