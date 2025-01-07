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
        precificacao_tarifador: STRING,
        precificacao_pre_pago: BOOLEAN,
        precificacao_negociavel: BOOLEAN,
        produto_adicional: BOOLEAN,
        produto_nome: STRING,
        produto_identificador_sydle: STRING,
        data_de_ativacao: TIMESTAMP,
        precificacao_pro_rata: BOOLEAN,
        precificacao_valor: DECIMAL(18,2),
        parcela_final: STRING,
        valor_parcela: DECIMAL(18,2),
        parcela_inicial: STRING
      >>"
    ) AS componentes

  FROM latest
),

exploded_data AS (
  SELECT
    id_contrato,
    item.precificacao_tarifador AS tarifador,
    item.precificacao_pre_pago,
    item.precificacao_negociavel,
    item.produto_adicional,
    item.parcela_final,
    item.valor_parcela,
    item.parcela_inicial,
    item.produto_nome,
    item.produto_identificador_sydle,
    item.data_de_ativacao,
    item.precificacao_pro_rata,
    item.precificacao_valor

  FROM parsed_data
    LATERAL VIEW EXPLODE(componentes) AS item
)

SELECT *
FROM exploded_data
