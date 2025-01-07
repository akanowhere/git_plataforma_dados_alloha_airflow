SELECT 
  C.id_contrato
  ,C.CODIGO_CONTRATO_AIR
  ,C.CODIGO_CONTRATO_IXC
  ,C.data_cancelamento
  ,C.data_ativacao
  ,C.motivo_cancelamento
  ,C.cidade
  ,C.estado
  ,COALESCE(F.canal, FO.canal) AS canal
  ,C.marca
  ,C.unidade
  --,C.regional
  ,C.produto
  ,C.pacote_tv
  ,C.dia_fechamento
  ,C.dia_vencimento
  ,C.aging
  ,C.segmento
  ,COALESCE(F.equipe,FO.equipe)  AS equipe
  ,C.tipo_cancelamento
  ,C.sub_motivo_cancelamento
  ,C.cancelamento_invol
  ,C.pacote_nome
  ,C.valor_final
  ,C.cluster
  ,C.fonte
  ,C.legado_sistema
  ,C.legado_id
  ,COALESCE(F.canal_tratado,FO.canal_tratado)  AS canal_tratado
  ,COALESCE(F.tipo_canal,FO.tipo_canal)  AS tipo_canal
  ,C.aging_meses_cat
  ,C.flg_fidelizado
  ,CO.id_cliente AS codigo_cliente_air
  FROM {{ get_catalogo('gold') }}.base.fato_cancelamento C
LEFT JOIN {{ get_catalogo('gold') }}.venda.dim_venda F 
  ON (F.id_contrato_air = C.CODIGO_CONTRATO_AIR AND F.id_contrato_air IS NOT NULL)
LEFT JOIN {{ get_catalogo('gold') }}.venda.dim_venda FO 
  ON (FO.id_contrato_air IS NULL AND FO.fonte = C.fonte AND FO.id_contrato_ixc = C.CODIGO_CONTRATO_IXC)
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato CO 
  ON (C.CODIGO_CONTRATO_AIR = CO.id_contrato_air)

WHERE C.data_cancelamento >= '2023-01-01'