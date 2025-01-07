{% set catalog_schema_table=source('hub_sva_public', 'orders') %}
{% set partition_column="id" %}
{% set order_column="updatedAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    transaction_id AS id_transacao,
    NULLIF(token, '') AS token,
    status,
    expected_status AS status_esperado,
    CAST(createdAt AS TIMESTAMP) AS data_criacao,
    CAST(updatedAt AS TIMESTAMP) AS data_atualizacao,
    customer_id,
    product_id,
    brand_id

  FROM latest
)

SELECT *
FROM
  transformed
