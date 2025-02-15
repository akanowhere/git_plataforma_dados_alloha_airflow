SELECT
  id AS id_contrato,
  id AS id_contrato_air,
  CASE
    WHEN (legado_sistema IS NOT NULL) THEN legado_id
  END AS id_contrato_legado,
  CAST(data_criacao AS TIMESTAMP) AS data_criacao,
  usuario_criacao,
  CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
  usuario_alteracao,
  excluido,
  versao AS VERSAO_CONTRATO_AIR,
  id_cliente,
  pacote_codigo,
  pacote_nome,
  valor_base,
  valor_final,
  status,
  id_vencimento,
  dia_vencimento,
  fechamento_vencimento,
  id_regra_suspensao,
  nome_suspensao,
  grupo_cliente_suspensao,
  dias_atraso_suspensao,
  id_endereco_cobranca,
  codigo_tipo_cobranca,
  unidade_atendimento,
  cancelamento_motivo,
  legado_id,
  legado_sistema,
  marcador,
  b2b,
  pme,
  telefone,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao,
  CAST(data_alteracao_vencimento AS TIMESTAMP) AS data_alteracao_vencimento,
  usuario_alteracao_vencimento,
  excluido_vencimento,
  ativo_vencimento,
  CAST(data_alteracao_suspensao AS TIMESTAMP) AS data_alteracao_suspensao,
  usuario_alteracao_suspensao,
  excluido_suspensao,
  ativo_suspensao,
  CAST(data_primeira_ativacao AS DATE) AS data_primeira_ativacao,
  CAST(data_cancelamento AS DATE) AS data_cancelamento,
  ADICIONAL_DESC,
  VALOR_PADRAO_PLANO,
  VALOR_ADICIONAIS,
  id_campanha,
  equipe,
  canal,
  CAST(data_venda AS DATE) AS data_venda,
  flg_fidelizado
FROM
  {{ get_catalogo('silver') }}.stage_contrato.vw_contrato