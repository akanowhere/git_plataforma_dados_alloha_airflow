{% set catalog_schema_table=source("air_comercial", "tbl_contrato") %}
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
    versao,
    id_cliente,
    pacote_codigo,
    pacote_nome,
    valor_base,
    valor_final,
    status,
    id_vencimento,
    id_regra_suspensao,
    id_endereco_cobranca,
    TRY_CAST(data_ultimo_faturamento AS DATE) AS data_ultimo_faturamento,
    TRY_CAST(data_proximo_reajuste AS DATE) AS data_proximo_reajuste,
    TRY_CAST(data_primeira_ativacao AS DATE) AS data_primeira_ativacao,
    TRY_CAST(data_cancelamento AS DATE) AS data_cancelamento,
    TRY_CAST(data_ativacao_agendada AS DATE) AS data_ativacao_agendada,
    codigo_tipo_cobranca,
    unidade_atendimento,
    cancelamento_motivo,
    versao_motivo,
    legado_id,
    legado_sistema,
    observacao_internet,
    observacao_telefonia,
    observacao_tv,
    marcador,
    b2b,
    integracao_status,
    integracao_mensagem,
    integracao_codigo,
    pme,
    reserva_connectmaster_cancelada,
    telefone

  FROM latest
)

SELECT *
FROM transformed
