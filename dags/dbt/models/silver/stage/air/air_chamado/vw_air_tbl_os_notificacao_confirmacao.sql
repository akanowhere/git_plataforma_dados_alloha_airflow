{% set catalog_schema_table=source("air_chamado", "tbl_os_notificacao_confirmacao") %}
{% set partition_column="id" %}
{% set order_column="horario_disparo" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    id_cliente,
    id_contrato,
    id_os,
    numero_disparo,
    TRY_CAST(horario_disparo AS TIMESTAMP) AS horario_disparo,
    status,
    message_id

  FROM latest
)

SELECT *
FROM transformed
