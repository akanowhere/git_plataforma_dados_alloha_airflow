SELECT DISTINCT
  stage_cliente.id_cliente,
  tipo AS tipo_telefone,
  numero,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS stage_cliente

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_cliente_telefones AS cliente_telefones
  ON stage_cliente.id_cliente = cliente_telefones.id_cliente
