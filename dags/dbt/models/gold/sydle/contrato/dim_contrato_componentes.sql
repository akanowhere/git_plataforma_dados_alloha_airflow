SELECT
  stg_contrato.id_contrato,
  tarifador,
  precificacao_pre_pago,
  precificacao_negociavel,
  produto_adicional,
  parcela_final,
  valor_parcela,
  parcela_inicial,
  produto_nome,
  produto_identificador_sydle,
  data_de_ativacao,
  precificacao_pro_rata,
  precificacao_valor,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao
FROM {{ get_catalogo('silver') }}.stage_sydle.vw_contrato_sydle AS stg_contrato
LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_contrato_componentes AS contrato_componentes
  ON stg_contrato.id_contrato = contrato_componentes.id_contrato
