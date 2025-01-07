WITH cte AS (
  SELECT
    id_remessa,
    ROW_NUMBER() OVER (PARTITION BY id_remessa ORDER BY data_integracao ASC) AS num
  FROM
    {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_integracao
),
filtereddata AS (
  SELECT *
  FROM
    {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_integracao AS remessa_integracao
  WHERE
    id_remessa IN (SELECT id_remessa FROM cte WHERE num = 1)
)


SELECT DISTINCT
  remessa_integracao.id_remessa,
  remessa_integracao.codigo_remessa,
  pagador_nome,
  pagador_documento,
  pagador_id_sydle,
  beneficiario_nome,
  beneficiario_documento,
  beneficiario_id_sydle,
  status_remessa,
  valor_remessa,
  valor_pago,
  valor_estornado,
  convenio_nome,
  convenio_identificador,
  convenio_ativo,
  convenio_emissor_nome,
  convenio_emissor_documento,
  convenio_emissor_id_sydle,
  cliente_nome,
  cliente_documento,
  cliente_id_sydle,
  grupo_de_forma_de_pagamento,
  condicao_de_pagamento_nome,
  condicao_de_pagamento_identificador,
  condicao_de_pagamento_parcela,
  data_vencimento,
  multa,
  juros,
  data_status,
  data_previsao_repasse,
  data_repasse,
  data_contestacao,
  valor_taxas,
  valor_liquido_repasse,
  retorno,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM filtereddata AS remessa_integracao
