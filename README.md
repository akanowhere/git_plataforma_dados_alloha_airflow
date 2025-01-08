[PYTHON__BADGE]:https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue
[DBT__BADGE]:https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white
[AIRFLOW__BADGE]:https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white

![python][PYTHON__BADGE]
![dbt][DBT__BADGE]
![airflow][AIRFLOW__BADGE]

# Platafora de dados Alloha

## Descrição
Este repositório faz parte de um projeto maior que consiste em dois repositórios diferentes. Dedicado à orquestrar os jobs dos pipelines com o **Airflow** e modelar com o **dbt**.

## Pré-requisitos
- Pyhton
- Airflow
- dbt

## Como usar
### Subindo o contâiner
Por padrão, a máquina do Airflow ficará ligada sempre.  
Caso seja necessário desligá-la, ao religar, o Airflow fica up automaticamente.  
Caso seja necessário reiniciar o container, sigo o passo a passo:  
1. Entre na AWS EC2 `plataforma-dados-alloha-airflow-prd`
2. Execute o Docker Compose para iniciar o ambiente:
```bash
cd plataforma-dados-alloha-airflow/

docker-compose down

docker-compose up -d
```
### Usando a ferramenta
1. Abra a AWS
2. Entre no serviço EC2
3. Copie o IP da máquina `plataforma-dados-alloha-airflow-prd`
4. Abra uma nova guia
5. Digite o IP:8080
6. Faça login

## Estrutura do repositório
```
plataforma-dados-alloha-airflow/
│
├── dags/
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── gold/
│   │   │   │   └── ... # arquivos .sql que geram produtos
│   │   │   └── silver/
│   │   │   │   └── ... # arquivos .sql que geram views
│   │   └── seeds/
│   │       └── ... # arquivos .csv de depara
│   └── include/
│       └── ... # arquivos .py de configuração, notificação
└── infra/
    └── ... # arquivos docker do Airflow
```

## ![dbt][DBT__BADGE] Data Build Tool
- **models**
    - Arquivos SQL que contêm os modelos transformacionais.
    - `schema.yml`: Especificações dos modelos, testes e documentação.
- **seeds**
    - Arquivos CSV que carregam dados estáticos ou de configuração.
- **macros**
    - Funções SQL reutilizáveis que podem ser usadas em vários modelos.
- **dbt_project.yml**
    - Arquivo de configuração principal do DBT.
- **profile.yml**
    - Configurações de perfil do DBT, como conexões com bancos de dados.

Referência: [<u>dbt documentation</u>](https://docs.getdbt.com/docs/build/documentation)

## ![airflow][AIRFLOW__BADGE] Apache Airflow
- **dags**
    - Diretório que contém os DAGs (Directed Acyclic Graphs) do Airflow, que definem os workflows.
- **astronomer cosmos**
    - Ferramenta para facilitar a execução de modelos DBT através de DAGs no Airflow.

Referências:
- [<u>airflow documentation</u>](https://airflow.apache.org/docs/)
- [<u>cosmos documentation</u>](https://astronomer.github.io/astronomer-cosmos/)

## Contribuição
Para contribuir, siga as boas práticas:
1. Clone o repositório localmente
```bash
git clone git@ssh.dev.azure.com:v3/allohafibra/BI-ALLOHA/plataforma-dados-alloha-airflow
```
2. Crie uma branch única para trabalhar na issue
```bash
git checkout dev

git pull origin dev

git branch -b dev-<seu_nome>-<tarefa_a_ser_realizada>
```
3. Atualize a sua branch conforme necessário com commits semânticos
```bash
git add . # isso compila todas as alterações, use com sabedoria

git commit -m "<categoria>: <comentário>"

git push origin dev-<seu_nome>-<tarefa_a_ser_realizada>
```
4. Quando finalizar as alterações, crie um PR (Pull Request) no [<u> Azure DevOps </u>](https://dev.azure.com/allohafibra/BI-ALLOHA/_git/plataforma-dados-alloha-airflow) e aguarde a Aprovação do Tech Lead
