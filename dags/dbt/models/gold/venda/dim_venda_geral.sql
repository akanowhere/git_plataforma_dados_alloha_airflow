WITH stg_venda_geral AS (

  SELECT

    id_venda,
    data_criacao,
    usuario_criacao,
    data_alteracao,
    usuario_alteracao,
    excluido,
    natureza,
    fase_atual,
    id_vendedor,
    id_campanha,
    id_contrato,
    id_regra_suspensao,
    id_vencimento,
    id_endereco,
    id_analise_credito,
    unidade_atendimento,
    cod_tipo_cobranca,
    data_venda,
    pacote_base_nome,
    pacote_base_codigo,
    pacote_valor_total,
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
    id_processo_venda_sydle,
    valor_referencia_b2b,
    fonte,
    data_extracao

  FROM {{ ref('vw_venda_air_geral') }}

)

SELECT *
FROM stg_venda_geral
