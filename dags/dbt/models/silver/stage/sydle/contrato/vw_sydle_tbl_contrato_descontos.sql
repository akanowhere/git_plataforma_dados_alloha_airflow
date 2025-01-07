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
      componentes,
      "ARRAY<STRUCT<
        desconto:
          ARRAY<STRUCT<
            precificacao_nome: STRUCT<pt: STRING>,
            precificacao_pre_pago: STRING,
            precificacao_negociavel: STRING,
            precificacao_parcela_inicial: STRING,
            precificacao_parcela_final: STRING,
            precificacao_pro_rata: STRING,
            nome: STRING,
            precificacao_valor: STRING,
            identificador: STRING
          >>,
        produto_nome: STRING,
        produto_identificador_sydle: STRING,
        dados_desconto:
          ARRAY<STRUCT<
            data_inicio: STRING,
            data_fim_fidelidade: STRING,
            data_termino: STRING
          >>
      >>"
    ) AS componentes

  FROM latest
),

exploded_data AS (
  SELECT
    id_contrato,
    item.produto_nome,
    item.produto_identificador_sydle,
    item_desconto.precificacao_pre_pago AS desconto_precificacao_pre_pago,
    item_desconto.precificacao_negociavel AS desconto_precificacao_negociavel,
    item_desconto.precificacao_parcela_inicial AS desconto_precificacao_parcela_inicial,
    item_desconto.precificacao_parcela_final AS desconto_precificacao_parcela_final,
    item_desconto.precificacao_pro_rata AS desconto_precificacao_pro_rata,
    item_desconto.nome AS desconto_nome,
    item_desconto.precificacao_nome.pt AS desconto_precificacao_nome,
    item_desconto.precificacao_valor AS desconto_precificacao_valor,
    item_desconto.identificador AS desconto_identificador,
    TRY_CAST(item_dados_desconto.data_inicio AS TIMESTAMP) AS data_inicio,
    TRY_CAST(item_dados_desconto.data_fim_fidelidade AS TIMESTAMP) AS data_fim_fidelidade,
    TRY_CAST(item_dados_desconto.data_termino AS TIMESTAMP) AS data_termino

  FROM parsed_data
    LATERAL VIEW EXPLODE(componentes) AS item
    LATERAL VIEW EXPLODE(item.desconto) AS item_desconto
    LATERAL VIEW EXPLODE(item.dados_desconto) AS item_dados_desconto
)

SELECT *
FROM exploded_data
