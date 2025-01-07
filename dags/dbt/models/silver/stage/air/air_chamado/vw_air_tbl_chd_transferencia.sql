{% set catalog_schema_table=source("air_chamado", "tbl_chd_transferencia") %}
{% set partition_column="id" %}
{% set order_column="data_transferencia" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(data_transferencia AS TIMESTAMP) AS data_transferencia,
    id_chamado,
    codigo_usuario,
    id_fila_origem,
    id_fila_destino

  FROM latest
)

SELECT *
FROM transformed
