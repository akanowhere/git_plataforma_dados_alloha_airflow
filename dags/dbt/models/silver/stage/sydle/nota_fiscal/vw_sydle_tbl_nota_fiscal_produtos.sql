{% set catalog_schema_table=source("sydle", "nota_fiscal") %}
{% set partition_column="id_nota_fiscal" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

parsed_data AS (
  SELECT
    id_nota_fiscal,
    FROM_JSON(
      itens,
      "ARRAY<STRUCT<
        valor_base: DOUBLE,
        produtos:
          ARRAY<STRUCT<
            nome: STRING,
            id: STRING
          >>,
        servico_tributavel_id: STRING,
        valor_total_dos_impostos: DOUBLE,
        descricao: STRING,
        servico_tributavel_nome: STRING
      >>"
    ) AS itens

  FROM latest
),

exploded_data AS (
  SELECT
    id_nota_fiscal,
    item.descricao AS descricao_produto,
    CONCAT_WS(', ',
      TRANSFORM(item.produtos, produto -> produto.nome)
    ) AS nome_produto, -- Concatenando nomes dos produtos
    item.valor_base AS valor_base_produto,
    item.servico_tributavel_id,
    item.servico_tributavel_nome,
    item.valor_total_dos_impostos AS valor_total_dos_impostos_do_produto

  FROM parsed_data
    LATERAL VIEW EXPLODE(itens) AS item
)

SELECT *
FROM exploded_data
