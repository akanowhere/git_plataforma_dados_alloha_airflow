{% set catalog_schema_table=source("sydle", "remessa") %}
{% set partition_column="id_remessa" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

parsed_data AS (
  SELECT
    id_remessa,
    FROM_JSON(
      faturas,
      "ARRAY<STRUCT<
        fatura_codigo: STRING,
        fatura_id: STRING
      >>"
    ) AS faturas

  FROM latest
),

exploded_data AS (
  SELECT
    id_remessa,
    item.fatura_codigo,
    item.fatura_id

  FROM parsed_data
    LATERAL VIEW EXPLODE(faturas) AS item
)

SELECT *
FROM exploded_data
