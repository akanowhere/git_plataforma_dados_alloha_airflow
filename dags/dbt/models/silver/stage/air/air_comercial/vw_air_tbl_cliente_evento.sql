{% set catalog_schema_table=source("air_comercial", "tbl_cliente_evento") %}
{% set partition_column="id" %}
{% set order_column="momento" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(momento AS TIMESTAMP) AS momento,
    usuario,
    id_contrato,
    id_cliente,
    tipo,
    observacao,
    motivo_associado

  FROM latest
)

SELECT *
FROM transformed
