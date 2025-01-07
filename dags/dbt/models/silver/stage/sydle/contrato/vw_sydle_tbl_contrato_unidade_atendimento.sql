{% set catalog_schema_table=source("sydle", "contrato") %}
{% set partition_column="id_contrato" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

parsed_data AS (
  SELECT
    id_contrato,
    FROM_JSON(
      unidade_atendimento_cidades_atendimento,
      "ARRAY<STRUCT<
        nome: STRING,
        codigo_ibge: STRING
      >>"
    ) AS unidade_atendimento_cidades_atendimento

  FROM latest
),

exploded_data AS (
  SELECT
    id_contrato,
    item.nome,
    item.codigo_ibge

  FROM parsed_data
    LATERAL VIEW EXPLODE(unidade_atendimento_cidades_atendimento) AS item
)

SELECT *
FROM exploded_data
