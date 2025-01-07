{% set catalog_schema_table=source("ecare", "onu") %}
{% set partition_column="id" %}
{% set order_column="updated_at" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    contract_id,
    customer_id,
    serial,
    pon,
    slot,
    olt,
    status,
    telephone,
    neighborhood,
    unity,
    region,
    manufacture,
    onu_index,
    rx_power,
    TRY_CAST(date_status AS TIMESTAMP) AS date_status,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    olt_ip

  FROM latest
)

SELECT *
FROM transformed
