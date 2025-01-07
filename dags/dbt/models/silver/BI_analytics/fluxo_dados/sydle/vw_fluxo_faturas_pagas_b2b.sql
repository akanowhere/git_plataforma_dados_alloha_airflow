WITH nf AS (
  SELECT
    fatura_codigo
  , CONCAT_WS(', ', COLLECT_LIST(numero)) AS numero_nf
FROM {{ get_catalogo('gold') }}.sydle.dim_nota_fiscal
GROUP BY fatura_codigo
)
SELECT
  'SYDLE' AS fonte,
  CASE
    WHEN F1.b2b = TRUE THEN 'B2B'
  END segmento,
  F2.contrato_air,
  F2.id_cliente_air,
  F2.codigo_fatura_sydle,
  F3.numero_nf,
  F2.status_fatura,
  F2.classificacao,
  F2.data_criacao,
  F2.data_vencimento,
  F2.data_pagamento,
  F2.valor_sem_multa_juros,
  F2.valor_pago,
  F2.beneficiario,
  F2.mes_referencia,
  F2.responsavel_registro_pagamento_manual_nome,
  F2.responsavel_registro_pagamento_manual_login,
  CAST(GETDATE() AS DATE) AS data_atualizacao_fluxo
FROM
  {{ get_catalogo('gold') }}.sydle.dim_faturas_mailing F2
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato F1 
         ON F1.id_contrato_air = F2.contrato_air
  LEFT JOIN nf F3
         ON F2.codigo_fatura_sydle = F3.fatura_codigo
WHERE
  F2.status_fatura IN ('PAGA')
  AND f1.b2b = TRUE