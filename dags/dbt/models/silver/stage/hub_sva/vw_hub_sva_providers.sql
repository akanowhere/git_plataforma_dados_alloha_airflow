{% set catalog_schema_table=source('hub_sva_public', 'providers') %}
{% set partition_column="id" %}
{% set order_column="updatedAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    name AS provedor,
    status,
    CAST(createdAt AS TIMESTAMP) AS data_criacao,
    CAST(updatedAt AS TIMESTAMP) AS data_atualizacao,
    access_token AS token_acesso,
    base_uri

  FROM latest
)

SELECT *
FROM
  transformed
