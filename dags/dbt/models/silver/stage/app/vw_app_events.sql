{# {% set catalog_schema_table=source('app', 'events') %}
{% set partition_column="session_id" %}
{% set order_column="createdAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
)

SELECT *
FROM
  latest #}

SELECT *
FROM {{ source('app', 'events') }}
