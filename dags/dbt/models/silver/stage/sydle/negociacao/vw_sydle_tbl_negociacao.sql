{% set catalog_schema_table=source("sydle", "negociacao") %}
{% set partition_column="id_registro" %}
{% set order_column="data_alteracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id_registro,
    TRY_CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
    faturas_negociadas,
    motivo_desconto,
    regenociacao,
    origem,
    valor_divida,
    valor_negociado,
    responsavel_negociacao,
    TO_DATE(data_negociacao, 'dd/MM/yyyy') AS data_negociacao,
    codigo_externo_contrato,
    faturas_geradas,
    id_plano_de_negociacao,
    tipo_de_acordo,
    parcelas,
    pedidos_negociados,
    quebra_de_acordo,
    pedidos_gerados

  FROM latest
)

SELECT *
FROM transformed
