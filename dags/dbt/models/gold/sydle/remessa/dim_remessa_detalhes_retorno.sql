SELECT DISTINCT
  remessa_integracao.id_remessa,
  remessa_detalhes_retorno.codigo_retorno,
  remessa_detalhes_retorno.nome_retorno,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_integracao AS remessa_integracao

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_detalhes_retorno AS remessa_detalhes_retorno
  ON remessa_integracao.id_remessa = remessa_detalhes_retorno.id_remessa
