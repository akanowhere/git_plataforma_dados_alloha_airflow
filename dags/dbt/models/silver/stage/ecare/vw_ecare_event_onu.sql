{% set catalog_schema_table=source("ecare", "event_onu") %}
{% set partition_column="id" %}
{% set order_column="updated_at" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    event_id,
    contract_id,
    nature_type,
    description,
    status,
    message,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    event_onu_status,
    onu_id,
    integrated,
    rx_power

  FROM latest
)

SELECT *
FROM transformed
