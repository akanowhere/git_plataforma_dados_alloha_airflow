SELECT

  ic.id,
  DATE_FORMAT(ic.data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
  ic.usuario_criacao,
  DATE_FORMAT(ic.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
  ic.usuario_alteracao,
  ic.excluido,
  ic.codigo,
  ic.nome,
  ic.descricao,
  ic.ordem,
  ic.cor,
  ic.icone,
  ic.ativo,
  c.id AS id_catalogo,
  DATE_FORMAT(c.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao_catalogo,
  c.usuario_alteracao AS usuario_alteracao_catalogo,
  c.excluido AS excluido_catalogo,
  c.codigo AS codigo_catalogo,
  c.nome AS nome_catalogo,
  c.descricao AS descricao_catalogo,
  c.ativo AS ativo_catalogo,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM {{ get_catalogo('silver') }}.stage.vw_air_tbl_catalogo_item AS ic
LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_catalogo AS c ON ic.id_catalogo = c.id
