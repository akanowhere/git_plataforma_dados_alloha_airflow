{% set catalog_schema_table=source("air_comercial", "tbl_desconto") %}
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
    desconto,
    TRY_CAST(data_validade AS DATE) AS data_validade,
    TRY_CAST(dia_aplicar AS DATE) AS dia_aplicar,
    item_codigo,
    item_nome,
    tipo,
    categoria,
    observacao

  FROM latest
)

SELECT *
FROM transformed
