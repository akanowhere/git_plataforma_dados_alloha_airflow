{% set catalog_schema_table=source("air_base", "tbl_usuario") %}
{% set partition_column="id" %}
{% set order_column="data_criacao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    codigo,
    nome,
    email,
    senha,
    administrador,
    ativo,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    TRY_CAST(data_validacao AS TIMESTAMP) AS data_validacao,
    token_validacao,
    url_foto,
    setor,
    pode_transferir

  FROM latest
)

SELECT *
FROM transformed
