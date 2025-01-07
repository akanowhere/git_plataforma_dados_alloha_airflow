{% set catalog_schema_table=source("air_comercial", "tbl_contrato_produto") %}
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
    id_contrato,
    versao_contrato,
    item_codigo,
    item_nome,
    item_grupo,
    TRY_CAST(data_instalacao AS TIMESTAMP) AS data_instalacao,
    TRY_CAST(data_cancelamento AS TIMESTAMP) AS data_cancelamento,
    adicional

  FROM latest
)

SELECT *
FROM transformed
