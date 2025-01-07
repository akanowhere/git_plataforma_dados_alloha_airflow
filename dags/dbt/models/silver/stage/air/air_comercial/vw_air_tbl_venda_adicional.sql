{% set catalog_schema_table=source("air_comercial", "tbl_venda_adicional") %}
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
    id_venda,
    produto_codigo,
    produto_nome,
    produto_grupo,
    valor_total,
    quantidade

  FROM latest
)

SELECT *
FROM transformed
