{% set catalog_schema_table=source("sydle", "usuario") %}
{% set partition_column="id_usuario" %}
{% set order_column="data_alteracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id_usuario,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    TRY_CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
    ativo,
    nome,
    login,
    email,
    TRY_CAST(data_integracao AS TIMESTAMP) AS data_integracao

  FROM latest
)

SELECT *
FROM transformed
