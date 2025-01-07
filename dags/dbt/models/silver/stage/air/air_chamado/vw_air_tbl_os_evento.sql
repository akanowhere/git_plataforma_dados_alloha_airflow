{% set catalog_schema_table=source("air_chamado", "tbl_os_evento") %}
{% set partition_column="id" %}
{% set order_column="momento" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(momento AS TIMESTAMP) AS momento,
    TRY_CAST(momento_evento AS TIMESTAMP) AS momento_evento,
    usuario,
    tipo,
    observacao,
    id_os

  FROM latest
)

SELECT *
FROM transformed
