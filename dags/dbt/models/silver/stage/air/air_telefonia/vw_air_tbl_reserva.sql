{% set catalog_schema_table=source("air_telefonia", "tbl_reserva") %}
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
    id_tarifacao,
    vsc_ctrl_number,
    unidade_sigla,
    chamada_cobrar,
    contrato_codigo,
    cliente_nome,
    credito_dia,
    credito_valor,
    habilitado,
    status_integracao,
    mensagem_integracao,
    vsc_senha,
    inbound_calls_limit,
    outbound_calls_limit

  FROM latest
)

SELECT *
FROM transformed
