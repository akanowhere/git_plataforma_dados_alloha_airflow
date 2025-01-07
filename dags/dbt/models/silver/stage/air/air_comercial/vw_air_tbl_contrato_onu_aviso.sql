{% set catalog_schema_table=source("air_comercial", "tbl_contrato_onu_aviso") %}
{% set partition_column="id" %}
{% set order_column="aviso_data" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(aviso_data AS TIMESTAMP) AS aviso_data,
    aviso_descricao,
    onu_serial,
    onu_olt,
    onu_slot,
    onu_pon,
    onu_index,
    onu_rx_power,
    onu_descricao,
    id_contrato,
    id_problema

  FROM latest
)

SELECT *
FROM transformed
