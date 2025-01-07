{% set catalog_schema_table=source("air_telefonia", "tbl_numero_portado") %}
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
    id_arealocal,
    TRY_CAST(data_portabilidade AS TIMESTAMP) AS data_portabilidade,
    devolvido,
    motivo,
    numero,
    operadora,
    id_reserva

  FROM latest
)

SELECT *
FROM transformed
