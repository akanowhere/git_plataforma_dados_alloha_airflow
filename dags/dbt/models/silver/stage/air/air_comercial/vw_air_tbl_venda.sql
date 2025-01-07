{% set catalog_schema_table=source("air_comercial", "tbl_venda") %}
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
    natureza,
    fase_atual,
    id_vendedor,
    id_campanha,
    id_lead,
    id_contrato,
    id_regra_suspensao,
    id_vencimento,
    id_endereco,
    id_analise_credito,
    unidade_atendimento,
    cod_tipo_cobranca,
    TRY_CAST(data_venda AS DATE) AS data_venda,
    observacao,
    portabilidade_numero,
    pacote_base_nome,
    pacote_base_codigo,
    pacote_valor_total,
    valor_comissao,
    concretizada,
    confirmada,
    valor_total,
    possui_internet,
    possui_tv,
    possui_telefone,
    quantidade_parcela_instalacao,
    cancelada,
    recorrencia_percentual_desconto,
    recorrencia_meses_desconto,
    equipe,
    upgrade_logico,
    possui_taxa_instalacao_antecipada,
    agencia,
    conta_corrente,
    id_processo_venda_sydle,
    valor_referencia_b2b

  FROM latest
)

SELECT *
FROM transformed
