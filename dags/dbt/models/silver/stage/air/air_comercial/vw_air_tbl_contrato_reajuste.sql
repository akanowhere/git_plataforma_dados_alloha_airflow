{% set catalog_schema_table=source("air_comercial", "tbl_contrato_reajuste") %}
{% set partition_column="id" %}
{% set order_column="data_processamento" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    id_contrato,
    TRY_CAST(data_processamento AS TIMESTAMP) AS data_processamento,
    valor_anterior,
    valor_indice,
    valor_final,
    versao_contrato,
    TRY_CAST(data_primeira_consulta AS TIMESTAMP) AS data_primeira_consulta,
    TRY_CAST(data_segunda_consulta AS TIMESTAMP) AS data_segunda_consulta

  FROM latest
)

SELECT *
FROM transformed
