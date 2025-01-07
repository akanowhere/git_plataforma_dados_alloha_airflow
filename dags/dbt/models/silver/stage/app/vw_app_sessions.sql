{# {% set catalog_schema_table=source('app', 'sessions') %}
{% set partition_column="sessions_id" %}
{% set order_column="createdAt" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
)

SELECT *
FROM
  latest #}

SELECT *
FROM {{ source('app', 'sessions') }}
