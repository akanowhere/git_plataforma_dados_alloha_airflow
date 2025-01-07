{% set catalog_schema_table=source("sumicity_db_assine", "user_contract") %}
{% set partition_column="id" %}
{% set order_column="hiring_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    user_id,
    checkout_id,
    client_air_id,
    TRY_CAST(hiring_date AS DATE) AS hiring_date,
    status,
    partner_id,
    contract_air_id,
    lomadee_partner_id

  FROM latest
)

SELECT *
FROM transformed
