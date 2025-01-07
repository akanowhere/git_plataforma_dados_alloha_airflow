SELECT DISTINCT
  stage_cliente.id_cliente,
  cidade_nome,
  tipo,
  complemento,
  numero,
  logradouro,
  bairro,
  estado_nome,
  referencia,
  cidade_codigo_ibge,
  estado_sigla,
  cep,
  pais,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS stage_cliente

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_cliente_enderecos AS cliente_enderecos
  ON stage_cliente.id_cliente = cliente_enderecos.id_cliente
