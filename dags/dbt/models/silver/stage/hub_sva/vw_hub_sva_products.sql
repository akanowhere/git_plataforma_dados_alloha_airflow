{% set catalog_schema_table=source('hub_sva_public', 'products') %}
{% set partition_column="id" %}
{% set order_column="updatedAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    name AS produto,
    product_id AS sku_produto,
    CAST(createdAt AS TIMESTAMP) AS data_criacao,
    CAST(updatedAt AS TIMESTAMP) AS data_atualizacao,
    from_json(parameters, 'STRUCT<depends_on: STRING, dependents: ARRAY<STRING>>') AS parametros,
    brand_id,
    provider_id

  FROM latest
)

SELECT *
FROM
  transformed
