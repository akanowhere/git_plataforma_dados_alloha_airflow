SELECT
  stg_contrato.id_contrato,
  produto_nome,
  produto_identificador_sydle,
  try_cast(desconto_precificacao_pre_pago AS BIGINT) AS desconto_precificacao_pre_pago,
  try_cast(desconto_precificacao_negociavel AS BIGINT) AS desconto_precificacao_negociavel,
  try_cast(desconto_precificacao_parcela_inicial AS BIGINT) AS desconto_precificacao_parcela_inicial,
  try_cast(desconto_precificacao_parcela_final AS BIGINT) AS desconto_precificacao_parcela_final,
  try_cast(desconto_precificacao_pro_rata AS BIGINT) AS desconto_precificacao_pro_rata,
  desconto_nome,
  desconto_precificacao_nome,
  try_cast(desconto_precificacao_valor AS DECIMAL(18,2)) AS desconto_precificacao_valor,
  desconto_identificador,
  try_cast(data_inicio AS TIMESTAMP) AS data_inicio,
  try_cast(data_fim_fidelidade AS TIMESTAMP) AS data_fim_fidelidade,
  try_cast(data_termino AS TIMESTAMP) AS data_termino,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao
FROM {{ get_catalogo('silver') }}.stage_sydle.vw_contrato_sydle AS stg_contrato
LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_contrato_descontos AS contrato_descontos
  ON stg_contrato.id_contrato = contrato_descontos.id_contrato
