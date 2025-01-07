SELECT DISTINCT
  remessa_integracao.id_remessa,
  TRY_CAST(REPLACE(REPLACE(remessa_parcelas.parcela_data_alteracao_status, 't', ' '), 'z', '') AS TIMESTAMP) AS parcela_data_alteracao_status,
  remessa_parcelas.parcela_numero,
  remessa_parcelas.parcela_valor,
  remessa_parcelas.parcela_status,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_integracao AS remessa_integracao

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_remessa_parcelas AS remessa_parcelas
  ON remessa_integracao.id_remessa = remessa_parcelas.id_remessa
