{% set catalog_schema_table=source('hub_sva_public', 'partners') %}
{% set partition_column="id" %}
{% set order_column="updatedAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    name AS polo,
    CAST(createdAt AS TIMESTAMP) AS data_criacao,
    CAST(updatedAt AS TIMESTAMP) AS data_atualizacao,
    email,
    events AS eventos,
    credentials AS credenciais,
    webhook_uri AS uri_webhook

  FROM latest
)

SELECT *
FROM
  transformed
