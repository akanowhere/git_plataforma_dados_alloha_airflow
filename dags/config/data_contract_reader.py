import os
from abc import ABC
from typing import Literal

import yaml


class DataContract:
    """
    Classe para lidar com o contrato de dados YAML.

    Attributes:
        path (str): O caminho para o arquivo YAML que contém o contrato de dados.

    Methods:
        _get_docs(): Método privado para carregar os documentos YAML do arquivo.
        get_databases_parameters(): Método público para obter parâmetros de bancos de dados com base em origem e widgets.
    """

    def __init__(
        self,
        frequency: Literal["hourly", "daily"] = None,
        db_name: Literal["air", "assine"] = None,
        path="/opt/airflow/dags/config/data_contract.yaml",
    ) -> None:
        self.path = path
        self.freq = frequency
        self.db = db_name
        self.env = os.getenv("env")

    def _get_docs(self) -> list[dict]:
        """
        Carrega os documentos YAML do arquivo.

        Returns:
            list[dict]: Uma lista contendo os documentos YAML carregados do arquivo.
        """
        data_contract = []
        with open(self.path, "r") as file:
            for doc in yaml.safe_load_all(file):
                data_contract.append(doc)

        return data_contract

    def get_cron(self) -> str:
        data_contract = self._get_docs()

        if self.env == "dev" and self.freq == "4x_hourly":
            return "0 11,15,19,21,23 * * *"
        if self.env == "dev":
            return "0 9 * * *"
        else:
            return data_contract[1]["pipelines"]["job-to-landing"]["inputs"][self.freq][
                "databases"
            ]["cron"]

    def get_landing_parameters(self) -> dict:
        data_contract = self._get_docs()

        catalog = (
            "bronze_dev"
            if self.env == "dev"
            else data_contract[1]["pipelines"]["job-to-landing"]["inputs"]["catalog"]
        )

        metadata = data_contract[1]["pipelines"]["job-to-landing"]["inputs"][self.freq][
            "databases"
        ]["metadata"]

        schemas = data_contract[1]["pipelines"]["job-to-landing"]["inputs"][self.freq][
            "databases"
        ][self.db]["schemas"]

        outputs = (
            f"s3://plataforma-dados-alloha-landing-{self.env}-637423315513/{{origin}}/{{schema}}/{{table}}/"
            if self.env == "dev"
            else data_contract[1]["pipelines"]["job-to-landing"]["outputs"]
        )

        return {
            "env": self.env,
            "catalog": catalog,
            "metadata": str(metadata),
            "schemas": str(schemas),
            "outputs": outputs,
        }

    def get_bronze_parameters(self) -> dict:
        data_contract = self._get_docs()

        input_path = (
            "s3://plataforma-dados-alloha-landing-dev-637423315513/{origin}/{schema}/{table}/"
            if self.env == "dev"
            else data_contract[1]["pipelines"]["job-to-bronze"]["inputs"]["s3"]
        )

        schemas = data_contract[1]["pipelines"]["job-to-bronze"]["inputs"][self.freq][
            "databases"
        ][self.db]["schemas"]

        outputs = (
            "{'s3': 's3://plataforma-dados-alloha-bronze-dev-637423315513/{origin}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze_dev', 'schema': '{schema}', 'table': '{table}'}}"
            if self.env == "dev"
            else data_contract[1]["pipelines"]["job-to-bronze"]["outputs"]
        )

        return {
            "env": self.env,
            "input_path": str(input_path),
            "schemas": str(schemas),
            "outputs": str(outputs),
        }

    def get_databricks_job_id(self, job_name=None):
        jobs_contract = []
        path_jobs = "/opt/airflow/dags/config/databricks_jobs.yaml"
        with open(path_jobs, "r") as file:
            for doc in yaml.safe_load_all(file):
                jobs_contract.append(doc)

        if job_name is not None:
            return {job_name: jobs_contract[0][job_name][self.env]}

        else:
            return {
                "landing": jobs_contract[0][self.db][self.freq]["landing"][
                    self.env
                ],
                "bronze": jobs_contract[0][self.db][self.freq]["bronze"][
                    self.env
                ],
            }


class Sqs:
    def __init__(self) -> None:
        self.data_contract = DataContract()._get_docs()
        self.env = os.getenv("env")

    def get_cron(self) -> str:
        if self.env == "dev":
            return "0 9 * * *"
        else:
            return self.data_contract[1]["pipelines"]["job-to-landing"]["inputs"][
                "daily"
            ]["sqs"]["cron"]

    def get_landing_params(self, table: str) -> dict:
        job_to_landing = self.data_contract[1]["pipelines"]["job-to-landing"]

        for queue in job_to_landing["inputs"]["daily"]["sqs"]["queues"]:
            if table in queue:
                queue_param = queue

        landing_params = {
            "queues": queue_param,
            "output": job_to_landing["outputs"],
        }

        if self.env == "dev":
            landing_params["output"] = landing_params["output"].replace(
                "landing", "landing-dev"
            )

        landing_params["queues"] = str([f"{self.env}-{landing_params['queues']}"])

        return landing_params

    def get_bronze_params(self, table: str) -> dict:
        job_to_bronze = self.data_contract[1]["pipelines"]["job-to-bronze"]

        for table_name in job_to_bronze["inputs"]["daily"]["sqs"]["tables"]:
            if table in table_name:
                table_param = table_name

        bronze_params = {
            "input": job_to_bronze["inputs"]["s3"],
            "tables": str([table_param]),
            "outputs": job_to_bronze["outputs"],
        }

        if self.env == "dev":
            bronze_params["input"] = bronze_params["input"].replace(
                "landing-637423315513", "landing-dev-637423315513"
            )

            bronze_params["outputs"]["s3"] = bronze_params["outputs"]["s3"].replace(
                "bronze-637423315513", "bronze-dev-637423315513"
            )

            bronze_params["outputs"]["unity_catalog"]["catalog"] = "bronze_dev"

        bronze_params["outputs"] = str(bronze_params["outputs"])

        return bronze_params

    def get_queues(self) -> list:
        job_to_landing = self.data_contract[1]["pipelines"]["job-to-landing"]
        queues = []

        for queue in job_to_landing["inputs"]["daily"]["sqs"]["queues"]:
            queues.append(queue.split("-")[-1])

        return queues

    def get_databricks_job_id(self):
        data_contract = DataContract(
            path="/opt/airflow/dags/config/databricks_jobs.yaml"
        )._get_docs()

        return {
            "landing": data_contract[0]["sqs"]["landing"][os.getenv("env")],
            "bronze": data_contract[0]["sqs"]["bronze"][os.getenv("env")],
        }


class ContractReader(ABC):
    """
    Classe base para ler contratos de dados a partir de arquivos YAML.

    Args:
        frequency (str): A frequência do processo (hourly ou daily).
        source_type (str): O tipo de origem dos dados (api, bigquery, database, firebase, sqs).
        data_source (str): O nome da fonte de dados.
        env (str): O ambiente configurado na variável de ambiente 'env'.
        yml_file (str, opcional): O caminho para o arquivo YAML do contrato de dados. O padrão é '/opt/airflow/dags/config/data_contract.yaml'.
    """

    def __init__(
        self,
        frequency: Literal["hourly", "daily"],
        source_type: Literal["api", "bigquery", "databases", "firebase", "sqs", "sftp"],
        data_source: str,
        env: Literal["dev", "prd"] = "dev",
        yml_file: str = "/opt/airflow/dags/config/data_contract.yaml",
    ):
        """Inicializa o leitor do contrato de dados com base na frequência, tipo de origem, fonte de dados e ambiente."""
        self.env = env
        self.frequency = frequency
        self.source_type = source_type
        self.data_source = data_source
        self.yml_file = yml_file
        self.data_contract = self._get_docs()
        self.job_to_landing = self.data_contract[1]["pipelines"]["job-to-landing"]
        self.job_to_bronze = self.data_contract[1]["pipelines"]["job-to-bronze"]

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

    def get_landing_parameters(self) -> dict:
        """Retorna os parâmetros da camada de landing definidos no contrato."""
        inputs = self.job_to_landing["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        catalog = "bronze_dev" if self.env == "dev" else inputs["catalog"]
        cron = data_source["cron"][self.frequency]
        metadata = data_source["metadata"]
        schemas = data_source["schemas"]
        outputs = (
            "s3://plataforma-dados-alloha-landing-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/"
            if self.env == "dev"
            else "s3://plataforma-dados-alloha-landing-637423315513/{source_type}/{data_source}/{schema}/{table}/"
        )

        return {
            "catalog": catalog,
            "cron": cron,
            "metadata": str(metadata),
            "schemas": str(schemas),
            "outputs": outputs,
        }

    def get_bronze_parameters(self) -> dict:
        """Retorna os parâmetros da camada bronze definidos no contrato."""
        inputs = self.job_to_bronze["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        input_path = (
            "s3://plataforma-dados-alloha-landing-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/"
            if self.env == "dev"
            else "s3://plataforma-dados-alloha-landing-637423315513/{source_type}/{data_source}/{schema}/{table}/"
        )
        schemas = data_source["schemas"]
        outputs = (
            "{'s3': 's3://plataforma-dados-alloha-bronze-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze_dev', 'schema': '{schema}', 'table': '{table}'}}"
            if self.env == "dev"
            else "{'s3': 's3://plataforma-dados-alloha-bronze-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze', 'schema': '{schema}', 'table': '{table}'}}"
        )

        return {
            "input_path": str(input_path),
            "schemas": str(schemas),
            "outputs": str(outputs),
        }


class AppContractReader(ContractReader):
    def __init__(
        self,
        frequency: Literal["hourly", "daily"],
        source_type: Literal["api", "bigquery", "database", "firebase", "sqs"],
        data_source: str,
        env: Literal["dev", "prod"] = "dev",
        yml_file: str = "/opt/airflow/dags/config/data_contract.yaml",
    ):
        super().__init__(frequency, source_type, data_source, env, yml_file)

    def get_landing_parameters(self) -> dict:
        inputs = self.job_to_landing["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        catalog = "bronze_dev" if self.env == "dev" else inputs["catalog"]
        cron = data_source["cron"][self.frequency]
        metadata = data_source["metadata"]
        schemas = data_source["schemas"]
        outputs = (
            f"s3://plataforma-dados-alloha-landing-{self.env}-637423315513/{{source_type}}/{{data_source}}/{{schema}}/{{table}}/"
            if self.env == "dev"
            else f"s3://plataforma-dados-alloha-landing-637423315513/{{source_type}}/{{data_source}}/{{schema}}/{{table}}/"
        )

        return {
            "catalog": catalog,
            "cron": cron,
            "metadata": str(metadata),
            "schemas": str(schemas),
            "outputs": outputs,
        }

    def get_bronze_parameters(self) -> dict:
        inputs = self.job_to_bronze["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        input_path = (
            "s3://plataforma-dados-alloha-landing-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/"
            if self.env == "dev"
            else "s3://plataforma-dados-alloha-landing-637423315513/{source_type}/{data_source}/{schema}/{table}/"
        )
        schemas = data_source["schemas"]
        outputs = (
            "{'s3': 's3://plataforma-dados-alloha-bronze-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze_dev', 'schema': '{schema}', 'table': '{table}'}}"
            if self.env == "dev"
            else "{'s3': 's3://plataforma-dados-alloha-bronze-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze', 'schema': '{schema}', 'table': '{table}'}}"
        )

        return {
            "input_path": str(input_path),
            "schemas": str(schemas),
            "outputs": str(outputs),
        }

    def get_databricks_job_id(self):
        data_contract = DataContract(
            path="/opt/airflow/dags/config/databricks_jobs.yaml"
        )._get_docs()

        return {
            "landing": data_contract[0][self.data_source]["landing"][self.env],
            "bronze": data_contract[0][self.data_source]["bronze"][self.env],
        }

    def get_cron(self) -> str:
        return self.job_to_landing["inputs"]["source_types"][self.source_type][
            "data_sources"
        ][self.data_source]["cron"][self.frequency]
    


class central_assinante_ContractReader(ContractReader):
    def __init__(
        self,
        frequency: Literal["hourly", "daily"],
        source_type: Literal["api", "bigquery", "database", "firebase", "sqs"],
        data_source: str,
        env: Literal["dev", "prod"] = "dev",
        yml_file: str = "/opt/airflow/dags/config/data_contract.yaml",
    ):
        super().__init__(frequency, source_type, data_source, env, yml_file)

    def get_landing_parameters(self) -> dict:
        inputs = self.job_to_landing["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        catalog = "bronze_dev" if self.env == "dev" else inputs["catalog"]
        cron = data_source["cron"][self.frequency]
        metadata = data_source["metadata"]
        schemas = data_source["schemas"]
        outputs = (
            f"s3://plataforma-dados-alloha-landing-{self.env}-637423315513/{{source_type}}/{{data_source}}/{{schema}}/{{table}}/"
            if self.env == "dev"
            else f"s3://plataforma-dados-alloha-landing-637423315513/{{source_type}}/{{data_source}}/{{schema}}/{{table}}/"
        )

        return {
            "catalog": catalog,
            "cron": cron,
            "metadata": str(metadata),
            "schemas": str(schemas),
            "outputs": outputs,
        }

    def get_bronze_parameters(self) -> dict:
        inputs = self.job_to_bronze["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        input_path = (
            "s3://plataforma-dados-alloha-landing-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/"
            if self.env == "dev"
            else "s3://plataforma-dados-alloha-landing-637423315513/{source_type}/{data_source}/{schema}/{table}/"
        )
        schemas = data_source["schemas"]
        outputs = (
            "{'s3': 's3://plataforma-dados-alloha-bronze-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze_dev', 'schema': '{schema}', 'table': '{table}'}}"
            if self.env == "dev"
            else "{'s3': 's3://plataforma-dados-alloha-bronze-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze', 'schema': '{schema}', 'table': '{table}'}}"
        )

        return {
            "input_path": str(input_path),
            "schemas": str(schemas),
            "outputs": str(outputs),
        }

    def get_databricks_job_id(self):
        data_contract = DataContract(
            path="/opt/airflow/dags/config/databricks_jobs.yaml"
        )._get_docs()

        return {
            "landing": data_contract[0][self.data_source]["landing"][self.env],
            "bronze": data_contract[0][self.data_source]["bronze"][self.env],
        }

    def get_cron(self) -> str:
        return self.job_to_landing["inputs"]["source_types"][self.source_type][
            "data_sources"
        ][self.data_source]["cron"][self.frequency]





class CleanupContractReader:
    def __init__(
        self,
        frequency: Literal["weekly", "daily"] = None,
        path="/opt/airflow/dags/config/data_contract.yaml",
    ) -> None:
        self.path = path
        self.freq = frequency
        self.env = os.getenv("env")

    def _get_docs(self) -> list[dict]:
        """
        Carrega os documentos YAML do arquivo.

        Returns:
            list[dict]: Uma lista contendo os documentos YAML carregados do arquivo.
        """
        data_contract = []
        with open(self.path, "r") as file:
            for doc in yaml.safe_load_all(file):
                data_contract.append(doc)

        return data_contract

    def get_cron(self) -> str:
        data_contract = self._get_docs()

        if self.env == "dev":
            return "0 19 * * 1"
        else:
            return data_contract[1]["pipelines"]["job-cleanup"]["inputs"][self.freq][
                "databases"
            ]["cron"]

    def get_cleanup_parameters(self) -> dict:
        data_contract = self._get_docs()

        retention_hours = data_contract[1]["pipelines"]["job-cleanup"]["inputs"][
            "retention_hours"
            ]
            
        catalogs = (
            ["bronze_dev","gold_dev"]
            if self.env == "dev"
            else data_contract[1]["pipelines"]["job-cleanup"]["inputs"][
                "unity_catalog"
            ]["catalogs"]
        )

        return {
            "env": self.env,
            "retention_hours": str(retention_hours),
            "catalogs": str(catalogs),
        }

    def get_databricks_job_id(self):
        data_contract = DataContract(
            path="/opt/airflow/dags/config/databricks_jobs.yaml"
        )._get_docs()

        return {
            "cleanup": data_contract[0]["cleanup"][os.getenv("env")],
            "cleanup_xcoms": data_contract[0]["cleanup_xcoms"][os.getenv("env")],
        }


class LojaContractReader(ContractReader):
    def __init__(
        self,
        frequency: Literal["hourly", "daily"],
        source_type: Literal[
            "api", "bigquery", "database", "firebase", "sqs", "google_cloud_sotarage"
        ],
        data_source: str,
        env: Literal["dev", "prod"] = "dev",
        yml_file: str = "/opt/airflow/dags/config/data_contract.yaml",
    ):
        super().__init__(frequency, source_type, data_source, env, yml_file)

    def get_landing_parameters(self) -> dict:
        inputs = self.job_to_landing["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        catalog = "bronze_dev" if self.env == "dev" else inputs["catalog"]
        cron = data_source["cron"][self.frequency]
        metadata = data_source["metadata"]
        schemas = data_source["schemas"]
        outputs = (
            f"s3://plataforma-dados-alloha-landing-{self.env}-637423315513/{{source_type}}/{{data_source}}/{{schema}}/{{table}}/"
            if self.env == "dev"
            else f"s3://plataforma-dados-alloha-landing-637423315513/{{source_type}}/{{data_source}}/{{schema}}/{{table}}/"
        )

        return {
            "catalog": catalog,
            "cron": cron,
            "metadata": str(metadata),
            "schemas": str(schemas),
            "outputs": outputs,
        }

    def get_bronze_parameters(self) -> dict:
        inputs = self.job_to_bronze["inputs"]
        data_source = inputs["source_types"][self.source_type]["data_sources"][
            self.data_source
        ]

        input_path = (
            "s3://plataforma-dados-alloha-landing-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/"
            if self.env == "dev"
            else "s3://plataforma-dados-alloha-landing-637423315513/{source_type}/{data_source}/{schema}/{table}/"
        )
        schemas = data_source["schemas"]
        outputs = (
            "{'s3': 's3://plataforma-dados-alloha-bronze-dev-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze_dev', 'schema': '{schema}', 'table': '{table}'}}"
            if self.env == "dev"
            else "{'s3': 's3://plataforma-dados-alloha-bronze-637423315513/{source_type}/{data_source}/{schema}/{table}/', 'unity_catalog': {'catalog': 'bronze', 'schema': '{schema}', 'table': '{table}'}}"
        )

        return {
            "input_path": str(input_path),
            "schemas": str(schemas),
            "outputs": str(outputs),
        }

    def get_databricks_job_id(self):
        data_contract = DataContract(
            path="/opt/airflow/dags/config/databricks_jobs.yaml"
        )._get_docs()

        return {
            "landing": data_contract[0][self.data_source]["landing"][self.env],
            "bronze": data_contract[0][self.data_source]["bronze"][self.env],
        }

    def get_cron(self) -> str:
        return self.job_to_landing["inputs"]["source_types"][self.source_type][
            "data_sources"
        ][self.data_source]["cron"][self.frequency]
