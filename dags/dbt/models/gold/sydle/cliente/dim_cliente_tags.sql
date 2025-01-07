SELECT DISTINCT
  stage_cliente.id_cliente,
  cliente_tags.nome AS tag,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS stage_cliente

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_cliente_tags AS cliente_tags
  ON stage_cliente.id_cliente = cliente_tags.id_cliente
