SELECT DISTINCT
  stage_cliente.id_cliente,
  cliente_redes_sociais.nome AS nome_rede_social,
  url,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS stage_cliente

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_cliente_redes_sociais AS cliente_redes_sociais
  ON stage_cliente.id_cliente = cliente_redes_sociais.id_cliente
