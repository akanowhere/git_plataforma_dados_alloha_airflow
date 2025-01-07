{% set catalog_schema_table=source("sumicity_db_notification", "contact") %}
{% set partition_column="id" %}
{% set order_column="updated_at" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    phone,
    customer_id,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    origin,
    favorite,
    deleted,
    authorized,
    NULL AS creation_user  -- REMOVER OS NULL CASO AS COLUNAS TENHAM DADOS

  FROM latest
)

SELECT *
FROM transformed
