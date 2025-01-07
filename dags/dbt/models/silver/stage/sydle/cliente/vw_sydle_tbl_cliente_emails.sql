{% set catalog_schema_table=source("sydle", "cliente") %}
{% set partition_column="id_cliente" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

parsed_data AS (
  SELECT
    id_cliente,
    FROM_JSON(
      emails,
      "ARRAY<STRUCT<
        tipo: STRING,
        email: STRING
      >>"
    ) AS emails

  FROM latest
),

exploded_data AS (
  SELECT
    id_cliente,
    item.tipo,
    item.email

  FROM parsed_data
    LATERAL VIEW EXPLODE(emails) AS item
)

SELECT *
FROM exploded_data
