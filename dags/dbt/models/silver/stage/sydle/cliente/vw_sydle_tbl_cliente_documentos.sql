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
      documentos,
      "ARRAY<STRUCT<
        documento_tipo: STRING,
        documento_numero: STRING
      >>"
    ) AS documentos

  FROM latest
),

exploded_data AS (
  SELECT
    id_cliente,
    item.documento_tipo,
    item.documento_numero

  FROM parsed_data
    LATERAL VIEW EXPLODE(documentos) AS item
)

SELECT *
FROM exploded_data
