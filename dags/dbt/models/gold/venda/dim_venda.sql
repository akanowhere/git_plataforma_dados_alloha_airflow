SELECT
  base.id_venda,
  data_venda,
  data_criacao,
  data_alteracao,
  usuario_criacao,
  usuario_alteracao,
  excluido,
  natureza,
  fase_atual,
  id_vendedor,
  id_campanha,
  id_lead,
  id_regra_suspensao,
  id_vencimento,
  unidade_atendimento,
  cod_tipo_cobranca,
  concretizada,
  confirmada,
  possui_internet,
  possui_tv,
  possui_telefone,
  recorrencia_percentual_desconto,
  recorrencia_meses_desconto,
  id_processo_venda_sydle,
  base.id_contrato,
  base.id_cliente,
  tipo_cliente,
  segmento,
  equipe,
  CASE
    WHEN cpm.cupom IS NOT NULL THEN 'INDIQUE UM AMIGO'
    ELSE canal
  END AS canal,
  unidade,
  CASE
    WHEN legado_sistema = 'ixc_click' THEN 'CLICK'
    ELSE UPPER(base.marca)
  END AS marca,
  tipo_residencia,
  COALESCE(
    CAST(sr.sigla_regional AS STRING),
    base.regional
  ) AS regional,
  pacote_base_nome,
  pacote_base_codigo,
  campanha_nome,
  campanha_codigo,
  valor_total,
  pacote_valor_total,
  desconto,
  valor_final_com_desconto,
  fonte,
  CAST(data_extracao AS TIMESTAMP) AS data_extracao,
  {{ translate_column('endereco_cobranca') }} AS endereco_cobranca,
  numero_cobranca,
  latitude_cobranca,
  longitude_cobranca,
  {{ translate_column('endereco_entrega') }} AS endereco_entrega,
  numero_entrega,
  latitude_entrega,
  longitude_entrega,
  id_endereco_entrega,
  id_endereco_cobranca,
  velocidade,
  produto,
  pacote_tv,
  cobranca,
  CASE
    WHEN equipe = 'EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA' THEN 'TERCEIRIZADO'
    WHEN consultor_autonomo = 1 THEN 'TERCEIRIZADO'
    ELSE tipo_canal
  END AS tipo_canal,
  COALESCE(afiliado_assine, 0) AS afiliado_assine,
  CASE
    WHEN equipe = 'EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA' THEN 1
    ELSE COALESCE(consultor_autonomo, 0)
  END AS consultor_autonomo,
  UPPER(
    CASE
      WHEN fonte NOT IN ('click', 'univox')
        AND afiliado_assine = 1 THEN 'AFILIADO'
      WHEN fonte NOT IN ('click', 'univox')
        AND consultor_autonomo = 1 THEN 'GIGA EMBAIXADOR'
      WHEN cpm.cupom IS NOT NULL THEN 'INDIQUE UM AMIGO'
      ELSE canal_tratado
    END
  ) AS canal_tratado,
  legado_id,
  legado_sistema,
  {{ translate_column('bairro') }} AS bairro,
  cep,
  {{ translate_column('base.cidade') }} AS cidade,
  {{ translate_column('estado') }} AS estado,
  CAST(
    CASE
      WHEN fonte = 'AIR' THEN id_contrato
      ELSE NULL
    END AS BIGINT
  ) AS id_contrato_air,
  CAST(
    CASE
      WHEN fonte <> 'AIR' THEN id_contrato
      ELSE NULL
    END AS BIGINT
  ) AS id_contrato_ixc,
  valor_referencia_b2b,
  UPPER(cpm.cupom) AS cupom,
  CASE
    WHEN (UPPER(cpm.cupom)) IN (
        SELECT UPPER(cupom)
        FROM {{ get_catalogo('silver') }}.stage_venda_hora.vw_air_cupom
      ) THEN 1
    ELSE 0
  END AS cupom_valido,
  sr.macro_regional,
  sr.regional1 AS nome_regional,
  duracao_desconto,
  cpf_cnpj
FROM
  (
    SELECT
      id_venda,
      data_venda,
      data_criacao,
      data_alteracao,
      usuario_criacao,
      usuario_alteracao,
      excluido,
      natureza,
      fase_atual,
      id_vendedor,
      id_campanha,
      id_lead,
      id_regra_suspensao,
      id_vencimento,
      unidade_atendimento,
      cod_tipo_cobranca,
      concretizada,
      confirmada,
      possui_internet,
      possui_tv,
      possui_telefone,
      recorrencia_percentual_desconto,
      recorrencia_meses_desconto,
      id_processo_venda_sydle,
      id_contrato,
      id_cliente,
      tipo_cliente,
      segmento,
      equipe,
      CASE
        WHEN canal = 'TLV RECEPTIVO' THEN 'RECEPTIVO'
        ELSE canal
      END AS canal,
      CASE
        WHEN canal = 'TLV RECEPTIVO' THEN 'RECEPTIVO'
        ELSE canal
      END AS canal_tratado,
      unidade,
      marca,
      tipo_residencia,
      regional,
      pacote_base_nome,
      pacote_base_codigo,
      campanha_nome,
      campanha_codigo,
      valor_total,
      pacote_valor_total,
      desconto,
      duracao_desconto,
      valor_final_com_desconto,
      fonte,
      data_extracao,
      endereco_cobranca,
      numero_cobranca,
      latitude_cobranca,
      longitude_cobranca,
      endereco_entrega,
      numero_entrega,
      latitude_entrega,
      longitude_entrega,
      id_endereco_entrega,
      id_endereco_cobranca,
      bairro_entrega AS bairro,
      cep_entrega AS cep,
      cidade,
      estado,
      velocidade,
      produto,
      pacote_tv,
      cobranca,
      tipo_canal,
      NULL AS afiliado_assine,
      CASE
        WHEN (
          SELECT MAX(aa.contract_air_id)
          FROM

            {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_consultor AS aa

          WHERE
            aa.contract_air_id = id_contrato
        ) IS NOT NULL THEN 1
        ELSE 0
      END AS consultor_autonomo,
      legado_id,
      legado_sistema,
      valor_referencia_b2b,
      cpf_cnpj
    FROM

      {{ ref('vw_venda_air') }}

  ) AS base

LEFT JOIN {{ get_catalogo('silver') }}.stage_auxiliar.vw_assine_cupom AS cpm

  ON cpm.contrato_air = CAST(
    CASE
      WHEN base.fonte = 'AIR' THEN base.id_contrato
      ELSE NULL
    END AS BIGINT
  )

LEFT JOIN {{ get_catalogo('silver') }}.stage_seeds_data.subregionais AS sr

  ON
  UPPER({{ translate_column('base.cidade') }}) = sr.cidade_sem_acento
  AND base.estado = UPPER(sr.uf)
  AND sr.marca = CASE
    WHEN base.marca = 'GIGA+ FIBRA' THEN 'GIGA+'
    WHEN base.marca = 'NIU' THEN 'NIU FIBRA'
    ELSE UPPER(base.marca)
  END

UNION ALL

SELECT
  TRY_CAST(id_venda AS BIGINT),
  TRY_CAST(data_venda AS DATE),
  TRY_CAST(data_criacao AS STRING),
  TRY_CAST(data_alteracao AS STRING),
  TRY_CAST(usuario_criacao AS STRING),
  TRY_CAST(usuario_alteracao AS STRING),
  FALSE AS excluido,
  TRY_CAST(natureza AS STRING),
  TRY_CAST(fase_atual AS STRING),
  TRY_CAST(id_vendedor AS BIGINT),
  TRY_CAST(id_campanha AS BIGINT),
  TRY_CAST(id_lead AS BIGINT),
  TRY_CAST(id_regra_suspensao AS BIGINT),
  TRY_CAST(id_vencimento AS BIGINT),
  TRY_CAST(unidade_atendimento AS STRING),
  TRY_CAST(cod_tipo_cobranca AS STRING),
  FALSE AS concretizada,
  FALSE AS confirmada,
  FALSE AS possui_internet,
  FALSE AS possui_tv,
  FALSE AS possui_telefone,
  TRY_CAST(recorrencia_percentual_desconto AS DOUBLE),
  TRY_CAST(recorrencia_meses_desconto AS BIGINT),
  TRY_CAST(id_processo_venda_sydle AS STRING),
  TRY_CAST(id_contrato AS BIGINT),
  TRY_CAST(id_cliente AS BIGINT),
  TRY_CAST(tipo_cliente AS STRING),
  TRY_CAST(segmento AS STRING),
  TRY_CAST(equipe AS STRING),
  TRY_CAST(canal AS STRING),
  TRY_CAST(unidade AS STRING),
  TRY_CAST(marca AS STRING),
  TRY_CAST(tipo_residencia AS STRING),
  TRY_CAST(regional AS STRING),
  TRY_CAST(pacote_base_nome AS STRING),
  TRY_CAST(pacote_base_codigo AS STRING),
  TRY_CAST(campanha_nome AS STRING),
  TRY_CAST(campanha_codigo AS STRING),
  TRY_CAST(valor_total AS DOUBLE),
  TRY_CAST(pacote_valor_total AS DOUBLE),
  TRY_CAST(desconto AS DOUBLE),
  TRY_CAST(valor_final_com_desconto AS DOUBLE),
  TRY_CAST(fonte AS STRING),
  TRY_CAST(data_extracao AS TIMESTAMP),
  TRY_CAST(endereco_cobranca AS STRING),
  TRY_CAST(numero_cobranca AS STRING),
  TRY_CAST(latitude_cobranca AS STRING),
  TRY_CAST(longitude_cobranca AS STRING),
  TRY_CAST(endereco_entrega AS STRING),
  TRY_CAST(numero_entrega AS STRING),
  TRY_CAST(latitude_entrega AS DOUBLE),
  TRY_CAST(longitude_entrega AS DOUBLE),
  TRY_CAST(id_endereco_entrega AS BIGINT),
  TRY_CAST(id_endereco_cobranca AS BIGINT),
  TRY_CAST(velocidade AS STRING),
  TRY_CAST(produto AS STRING),
  TRY_CAST(pacote_tv AS STRING),
  TRY_CAST(cobranca AS STRING),
  TRY_CAST(tipo_canal AS STRING),
  TRY_CAST(afiliado_assine AS INTEGER),
  TRY_CAST(consultor_autonomo AS INTEGER),
  TRY_CAST(canal_tratado AS STRING),
  TRY_CAST(legado_id AS BIGINT),
  TRY_CAST(legado_sistema AS STRING),
  TRY_CAST(bairro AS STRING),
  TRY_CAST(cep AS STRING),
  TRY_CAST(cidade AS STRING),
  TRY_CAST(estado AS STRING),
  TRY_CAST(id_contrato_air AS BIGINT),
  TRY_CAST(id_contrato_ixc AS BIGINT),
  TRY_CAST(valor_referencia_b2b AS DOUBLE),
  NULL AS cupom,
  TRY_CAST(cupom_valido AS INTEGER),
  TRY_CAST(macro_regional AS STRING),
  TRY_CAST(nome_regional AS STRING),
  TRY_CAST(duracao_desconto AS BIGINT),
  TRY_CAST(cpf_cnpj AS STRING)
FROM

  {{ get_catalogo('silver') }}.stage_legado.vw_dim_venda
