SELECT DISTINCT
  stage_cliente.id_cliente,
  tipo,
  email,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS stage_cliente

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_cliente_emails AS cliente_emails
  ON stage_cliente.id_cliente = cliente_emails.id_cliente
