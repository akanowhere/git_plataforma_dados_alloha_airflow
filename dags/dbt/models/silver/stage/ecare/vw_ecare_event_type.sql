{% set catalog_schema_table=source("ecare", "event_type") %}
{% set partition_column="id" %}
{% set order_column="created_at" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    key,
    description,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    has_incident

  FROM latest
)

SELECT *
FROM transformed
