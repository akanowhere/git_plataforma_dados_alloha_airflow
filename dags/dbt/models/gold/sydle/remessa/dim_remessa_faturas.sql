SELECT DISTINCT
  remessa_integracao.id_remessa,
  remessa_faturas.fatura_codigo,
  remessa_faturas.fatura_id,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_integracao AS remessa_integracao

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_faturas AS remessa_faturas
  ON remessa_integracao.id_remessa = remessa_faturas.id_remessa
