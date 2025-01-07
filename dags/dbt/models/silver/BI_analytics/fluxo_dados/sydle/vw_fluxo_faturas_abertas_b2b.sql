SELECT
  'SYDLE' as fonte,
  CASE
    WHEN F1.b2b = TRUE THEN 'B2B'
  END segmento,
  F2.contrato_air,
  F2.id_cliente_air,
  F2.codigo_fatura_sydle,
  F2.status_fatura,
  F2.classificacao,
  F2.data_criacao,
  F2.data_vencimento,
  F2.data_pagamento,
  F2.valor_sem_multa_juros,
  F2.valor_pago,
  F2.beneficiario,
  F2.mes_referencia,
  CAST(GETDATE() AS DATE) AS data_atualizacao_fluxo
FROM
  {{ get_catalogo('gold') }}.sydle.dim_faturas_mailing F2
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato F1 
    ON F1.id_contrato_air = F2.contrato_air
WHERE
  F2.status_fatura IN (
    'EMITIDA',
    'EM EMISSAO',
    'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO',
    'AGUARDANDO BAIXA EM SISTEMA EXTERNO'
  )
  AND f1.b2b = TRUE