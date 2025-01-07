{% set catalog_schema_table=source('hub_sva_public', 'brands') %}
{% set partition_column="id" %}
{% set order_column="updatedAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    name AS marca,
    CAST(createdAt AS TIMESTAMP) AS data_criacao,
    CAST(updatedAt AS TIMESTAMP) AS data_atualizacao,
    watch_id AS id_watch,
    watch_redirect_url AS url_watch,
    watch_secret AS senha_watch,
    youcast_user AS usuario_youcast,
    youcast_secret AS senha_youcast,
    partner_id

  FROM latest
)

SELECT *
FROM
  transformed
