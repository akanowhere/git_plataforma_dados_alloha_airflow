{% set catalog_schema_table=source("air_comercial", "tbl_contrato_retencao") %}
{% set partition_column="id" %}
{% set order_column="data_alteracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    usuario_criacao,
    TRY_CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
    usuario_alteracao,
    excluido,
    id_contrato,
    quantidade_contato,
    status,
    usuario_atribuido,
    motivo_solicitacao,
    oferta_incentivo,
    setor_origem,
    observacao,
    motivo_investigado,
    TRY_CAST(data_reversao_cancelamento AS TIMESTAMP) AS data_reversao_cancelamento,
    desconto_aplicado,
    sub_motivo_cancelamento,
    origem_contato

  FROM latest
)

SELECT *
FROM transformed
