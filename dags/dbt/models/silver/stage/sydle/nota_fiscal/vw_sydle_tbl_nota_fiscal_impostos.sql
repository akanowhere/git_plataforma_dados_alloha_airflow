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
        impostos_retidos: INT,
        valor_total_dos_impostos: DOUBLE,
        impostos:
          ARRAY<STRUCT<
            base_de_calculo: STRING,
            aliquota: STRING,
            imposto: STRING,
            valor: DOUBLE,
            retido: BOOLEAN,
            exigibilidade: STRING
          >>,
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
    item.valor_total_dos_impostos AS valor_total_dos_impostos_do_produto,
    item_imposto.imposto,
    item_imposto.base_de_calculo,
    item_imposto.aliquota,
    item_imposto.valor,
    item_imposto.retido,
    item.impostos_retidos,
    item_imposto.exigibilidade

  FROM parsed_data
    LATERAL VIEW EXPLODE(itens) AS item
    LATERAL VIEW EXPLODE(item.impostos) AS item_imposto
)

SELECT *
FROM exploded_data
