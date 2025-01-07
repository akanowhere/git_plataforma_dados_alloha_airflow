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
      detalhes_retorno,
      "ARRAY<STRUCT<
        codigo: STRING,
        nome: STRING
      >>"
    ) AS detalhes_retorno

  FROM latest
),

exploded_data AS (
  SELECT
    id_remessa,
    item.codigo AS codigo_retorno,
    item.nome AS nome_retorno

  FROM parsed_data
    LATERAL VIEW EXPLODE(detalhes_retorno) AS item
)

SELECT *
FROM exploded_data
