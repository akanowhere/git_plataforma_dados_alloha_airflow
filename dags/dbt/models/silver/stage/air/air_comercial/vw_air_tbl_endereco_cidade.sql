{% set catalog_schema_table=source("air_comercial", "tbl_endereco_cidade") %}
{% set partition_column="id" %}
{% set order_column="data_alteracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    usuario_criacao,
    TRY_CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
    usuario_alteracao,
    excluido,
    nome,
    estado,
    codigo_ibge,
    ddd,
    atendida

  FROM latest
)

SELECT *
FROM transformed
