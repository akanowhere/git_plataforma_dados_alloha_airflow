SELECT
  DISTINCT C.id,
  DATE_FORMAT(C.data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
  C.usuario_criacao,
  DATE_FORMAT(C.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
  C.usuario_alteracao,
  C.excluido,
  C.versao,
  C.id_cliente,
  C.pacote_codigo,
  C.pacote_nome,
  C.valor_base,
  C.valor_final,
  C.status,
  C.id_vencimento,
  DATE_FORMAT(VENCIMENTO.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao_vencimento,
  VENCIMENTO.usuario_alteracao AS usuario_alteracao_vencimento,
  VENCIMENTO.excluido AS excluido_vencimento,
  VENCIMENTO.dia AS dia_vencimento,
  VENCIMENTO.fechamento AS fechamento_vencimento,
  VENCIMENTO.ativo AS ativo_vencimento,
  C.id_regra_suspensao,
  SUSPENSAO.nome AS nome_suspensao,
  DATE_FORMAT(SUSPENSAO.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao_suspensao,
  SUSPENSAO.usuario_alteracao AS usuario_alteracao_suspensao,
  SUSPENSAO.excluido AS excluido_suspensao,
  SUSPENSAO.grupo_cliente AS grupo_cliente_suspensao,
  SUSPENSAO.dias_atraso AS dias_atraso_suspensao,
  SUSPENSAO.ativo AS ativo_suspensao,
  C.id_endereco_cobranca,
  C.codigo_tipo_cobranca,
  C.unidade_atendimento,
  C.cancelamento_motivo,
  C.legado_id,
  C.legado_sistema,
  C.marcador,
  C.b2b,
  C.pme,
  C.telefone,
  DATE_FORMAT(C.data_primeira_ativacao, 'yyyy-MM-dd') AS data_primeira_ativacao,
  DATE_FORMAT(C.data_cancelamento, 'yyyy-MM-dd') AS data_cancelamento,
  NOW() AS data_extracao,
  vlr.adicionais_nome AS ADICIONAL_DESC,
  vlr.valor_plano AS VALOR_PADRAO_PLANO,
  vlr.valor_adicionais AS VALOR_ADICIONAIS,
  v.id_campanha,
  REPLACE(v.equipe, 'EQUEQUIPE_', 'EQUIPE_') AS equipe,
  CASE
    WHEN v.equipe IN ('EQUIPE_PAP', 'EQUIPE_PAP_BRASILIA') THEN 'PAP DIRETO'
    WHEN v.equipe IN (
      'EQUIPE_AVANT_TELECOM_EIRELI',
      'EQUIPE_BLISS_TELECOM',
      'EQUIPE_FIBRA_TELECOM',
      'EQUIPE_RR_TELECOM',
      'EQUIPE_MAKOTO_TELECOM',
      'EQUIPE_MORANDI',
      'EQUIPE_LITORAL_SAT'
    ) THEN 'PAP INDIRETO'
    WHEN v.equipe LIKE '%PAP%' THEN 'PAP INDIRETO'
    WHEN v.equipe LIKE '%AGENTE_DIGITAL%' THEN 'AGENTE DIGITAL'
    WHEN v.equipe LIKE '%DIGITAL_ASSINE%' THEN 'ASSINE'
    WHEN v.equipe LIKE 'EQUIPE_DIGITAL'
    OR v.equipe LIKE 'EQUIPE_DIGITAL_BRA%'
    OR v.equipe LIKE '%COM_DIGITAL%' THEN 'DIGITAL'
    WHEN v.equipe LIKE '%LOJA%' THEN 'LOJA'
    WHEN v.equipe LIKE '%OUTBOUND%' THEN 'OUTBOUND'
    WHEN v.equipe LIKE '%OUVIDORIA%' THEN 'OUVIDORIA'
    WHEN v.equipe LIKE '%RETENCAO%' THEN 'RETENCAO'
    WHEN v.equipe LIKE '%CORPORATIVO%' THEN 'CORPORATIVO'
    WHEN v.equipe LIKE '%COBRANCA%' THEN 'COBRANCA'
    WHEN v.equipe LIKE '%MIGRACAO%' THEN 'MIGRACAO'
    WHEN v.equipe LIKE '%RH%' THEN 'RH'
    WHEN v.equipe LIKE '%TLV_ATIVO%' THEN 'DIGITAL'
    WHEN v.equipe LIKE '%RECEP%'
    OR v.equipe LIKE '%CALL_VENDAS%'
    OR v.equipe LIKE '%TELEVENDAS%' THEN 'TLV RECEPTIVO'
    WHEN v.equipe LIKE '%PRODUTOS_MKT%' THEN 'MARKETING'
    WHEN v.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_CLICK%' THEN 'CONSULTOR INDEPENDENTE'
    WHEN v.equipe LIKE '%EQUIPE_CONSULTOR_AUTONOMO_GIGA+FIBRA%' THEN 'CONSULTOR INDEPENDENTE'
    ELSE 'SEM ALOCAÇÃO'
  END AS canal,
  v.data_venda,CASE
    WHEN C.data_criacao is not null
    AND C.data_cancelamento is not null
    AND C.legado_sistema IS NOT NULL
    AND C.data_cancelamento < C.data_criacao THEN 'Sem Informacao'
    WHEN C.data_primeira_ativacao is not null
    and desconto.id_contrato IS NOT null THEN 'sim'
    WHEN C.data_primeira_ativacao is not null
    and mig.natureza = 'VN_MIGRACAO_COMPULSORIA' THEN 'nao'
    WHEN C.data_primeira_ativacao is not null
    and C.`status` <> 'ST_CONT_CANCELADO'
    and (
      mig.natureza <> 'VN_MIGRACAO_COMPULSORIA'
      OR mig.natureza IS NULL
    )
    AND DATEDIFF(
      CAST(NOW() AS DATE),
      ifnull(mig.data_venda, C.data_primeira_ativacao)
    ) BETWEEN 0
    AND 365 THEN 'sim'
    WHEN C.data_primeira_ativacao is not null
    and C.`status` = 'ST_CONT_CANCELADO'
    and (
      mig.natureza <> 'VN_MIGRACAO_COMPULSORIA'
      OR mig.natureza IS NULL
    )
    AND DATEDIFF(
      C.data_cancelamento,
      ifnull(mig.data_venda, C.data_primeira_ativacao)
    ) BETWEEN 0
    AND 365 THEN 'sim'
    ELSE 'nao'
  END AS flg_fidelizado
FROM
   {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato C
  LEFT JOIN  {{ get_catalogo('silver') }}.stage.vw_air_tbl_venda v ON (
    C.id = v.id_contrato
    AND v.natureza = 'VN_NOVO_CONTRATO'
  )
  LEFT JOIN  {{ get_catalogo('silver') }}.stage.vw_air_tbl_vencimento VENCIMENTO ON VENCIMENTO.id = C.id_vencimento
  LEFT JOIN  {{ get_catalogo('silver') }}.stage.vw_air_tbl_regra_suspensao SUSPENSAO ON SUSPENSAO.id = C.id_regra_suspensao
  LEFT JOIN (
    SELECT
      t0.id AS contrato_air,
      SUM(
        CASE
          WHEN t1.adicional = true THEN t2.valor_final
          ELSE 0
        END
      ) AS valor_adicionais,
      SUM(
        CASE
          WHEN t1.adicional = false THEN t2.valor_final
          ELSE 0
        END
      ) AS valor_plano,
      array_join(
        collect_list(
          CASE
            WHEN t1.adicional = true THEN IFNULL(t2.item_nome, t1.item_nome)
            ELSE NULL
          END
        ),
        ', '
      ) AS adicionais_nome
    FROM
       {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato t0
      LEFT JOIN  {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_produto t1 ON (
        t0.id = t1.id_contrato
        AND t0.versao = t1.versao_contrato and t1.excluido = FALSE
      )
      LEFT JOIN  {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_item t2 ON (
        t1.id = t2.id_contrato_produto
        AND t2.excluido = false
      )
    GROUP BY
      t0.id
  ) vlr ON (C.id = vlr.contrato_air)
  LEFT JOIN (
    SELECT
      cm.id_contrato,
      cm.novo_versao,
      v.natureza,
      v.equipe,
      v.data_venda
    FROM
       {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_migracao cm
      LEFT JOIN  {{ get_catalogo('silver') }}.stage.vw_air_tbl_venda v on (cm.id_venda = v.id)
    WHERE
      cm.reprovada = false
      AND cm.id = (
        SELECT
          MAX(id)
        FROM
           {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_migracao
        WHERE
          reprovada = false
          and id_contrato = cm.id_contrato
      )
  ) mig ON (
    mig.id_contrato = C.id
    AND mig.novo_versao = C.versao
  )
  LEFT JOIN (
    SELECT
      d.id,
      d.id_contrato,
      d.desconto,
      d.dia_aplicar,
      d.data_validade
    FROM
       {{ get_catalogo('silver') }}.stage.vw_air_tbl_desconto d
    WHERE
      d.categoria = 'RETENCAO'
      AND cast(d.dia_aplicar AS DATE) >= '2023-07-01'
      AND d.id IN (
        SELECT
          MAX(id)
        FROM
           {{ get_catalogo('silver') }}.stage.vw_air_tbl_desconto
        WHERE
          categoria = 'RETENCAO'
          AND cast(dia_aplicar AS DATE) >= '2023-07-01'
          AND id_contrato = d.id_contrato
      )
      AND d.id NOT IN (

        SELECT
          id
        FROM
           {{ get_catalogo('silver') }}.stage.vw_air_tbl_desconto
        WHERE
          categoria = 'RETENCAO'
          AND cast(dia_aplicar AS DATE) >= '2023-07-01'
          AND (
            desconto BETWEEN 09
            AND 11
            AND DATEDIFF(data_validade, dia_aplicar) < 55
          )
      )
  ) desconto ON desconto.id_contrato = C.id
