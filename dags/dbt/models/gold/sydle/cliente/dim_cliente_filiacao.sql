SELECT DISTINCT
  stage_cliente.id_cliente,
  grau_parentesco,
  cliente_filiacao.nome AS nome_parente,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage_sydle.vw_cliente AS stage_cliente

LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_cliente_filiacao AS cliente_filiacao
  ON stage_cliente.id_cliente = cliente_filiacao.id_cliente
