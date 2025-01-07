{% set catalog_schema_table=source("sydle", "venda_qualify_vendedor") %}
{% set partition_column="id_sydle_one" %}
{% set order_column="data_integracao" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    id_sydle_one,
    nome_aprovacao,
    status_aprovacao,
    nome_aprovador_final_da_venda,
    executor_aprovacao,
    TRY_CAST(data_atendimento AS TIMESTAMP) AS data_atendimento,
    TRY_CAST(data_conclusao AS TIMESTAMP) AS data_conclusao,
    duracao_total,
    duracao_atendimento,
    canal_venda,
    TRY_CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    TRY_CAST(data_venda AS TIMESTAMP) AS data_venda,
    aprovado_pela_auditoria,
    venda_refeita,
    justificativa_aprovacao,
    justificativa_aprovacao_id_sydle_one,
    status_processo,
    analise_financeira,
    id_prospecto,
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
    TRY_CAST(data_ativacao AS TIMESTAMP) AS data_ativacao,
    TRY_CAST(data_integracao AS TIMESTAMP) AS data_integracao,
    DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

  FROM latest
)

SELECT *
FROM transformed
