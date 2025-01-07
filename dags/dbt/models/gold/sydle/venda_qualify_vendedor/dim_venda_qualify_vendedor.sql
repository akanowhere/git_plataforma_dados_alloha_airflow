WITH cte AS (
  SELECT
    id_sydle_one,
    ROW_NUMBER() OVER (PARTITION BY id_sydle_one ORDER BY data_integracao ASC) AS num
  FROM
    {{ get_catalogo('silver') }}.stage_sydle.vw_venda_qualify_vendedor
),
filtereddata AS (
  SELECT *
  FROM
    {{ get_catalogo('silver') }}.stage_sydle.vw_venda_qualify_vendedor AS venda_qualify_vendedor
  WHERE
    id_sydle_one IN (SELECT id_sydle_one FROM cte WHERE num = 1)
)

SELECT DISTINCT
  venda_qualify_vendedor.id_sydle_one,
  nome_aprovacao,
  status_aprovacao,
  nome_aprovador_final_da_venda,
  executor_aprovacao,
  data_atendimento,
  data_conclusao,
  duracao_total,
  duracao_atendimento,
  canal_venda,
  data_criacao,
  data_venda,
  aprovado_pela_auditoria,
  venda_refeita,
  justificativa_aprovacao,
  justificativa_aprovacao_id_sydle_one,
  status_processo,
  analise_financeira,
  prospecto,
  endereco_instalacao_estado,
  endereco_instalacao_cidade,
  endereco_instalacao_bairro,
  endereco_instalacao_logradouro,
  endereco_instalacao_numero,
  oferta_selecionada,
  parceiro,
  responsavel_pela_venda_id_sydle_one,
  responsavel_pela_venda_nome,
  condominio_nome,
  condominio_id_sydle_one,
  cpf,
  cnpj,
  contrato_codigo,
  status_venda,
  resultado_analise_financeira_valor_maximo_restricoes_spc_soma,
  resultado_analise_financeira_quantidade_maxima_restricoes_spc,
  resultado_analise_financeira_aprovado_serasa,
  resultado_analise_financeira_classificacao_minima_serasa,
  regional_nome,
  regional_id_sydle_one,
  telefone_utilizado_na_venda,
  air_codigo_contrato,
  responsavel_analise_financeira_interna_nome,
  responsavel_analise_financeira_interna_id_sydle_one,
  responsavel_analise_financeira_externa_nome,
  responsavel_analise_financeira_externa_id_sydle_one,
  responsavel_analise_tecnica_nome,
  responsavel_analise_tecnica_id_sydle_one,
  marca_associada,
  unidade_nome,
  unidade_sigla,
  motivo_auditoria_qualify,
  valor_total_mensalidade_com_desconto,
  data_ativacao,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao,
  id_prospecto
FROM
  filtereddata AS venda_qualify_vendedor;
