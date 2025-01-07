{% set catalog_schema_table=source("sydle", "fatura") %}
{% set partition_column="id_fatura" %}
{% set order_column="data_atualizacao DESC, data_extracao DESC" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

parsed_data AS (
  SELECT
    id_fatura,
    codigo,
    FROM_JSON(
      descricao_itens,
      "ARRAY<STRUCT<
        nome_item: STRING,
        valor_item: DOUBLE,
        tipo_item: STRING,
        data_fim_item: TIMESTAMP,
        data_inicio_item: TIMESTAMP
      >>"
    ) AS descricao_itens

  FROM latest
),

exploded_data AS (
  SELECT
    id_fatura,
    codigo AS codigo_fatura_sydle,
    item.nome_item,
    item.valor_item,
    item.tipo_item,
    item.data_inicio_item,
    item.data_fim_item

  FROM parsed_data
    LATERAL VIEW EXPLODE(descricao_itens) AS item
)

SELECT *
FROM exploded_data
