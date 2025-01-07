SELECT DISTINCT
  stage_cliente.id_cliente,
  documento_tipo,
  documento_numero,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS stage_cliente

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_cliente_documentos AS cliente_documentos
  ON stage_cliente.id_cliente = cliente_documentos.id_cliente
