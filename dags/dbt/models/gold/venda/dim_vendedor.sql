SELECT DISTINCT
  v.id AS id_vendedor,
  DATE_FORMAT(v.data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
  DATE_FORMAT(v.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
  v.excluido,
  REPLACE(v.equipe, 'EQUEQUIPE_', 'EQUIPE_') AS equipe,
  v.terceirizada,
  CAST(v.ativo AS CHAR(1)) AS ativo,
  v.id_vendedor_sydle,
  v.usuario,
  u.id AS id_usuario,
  u.codigo,
  u.nome,
  u.email,
  u.ativo AS usuario_ativo,
  DATE_FORMAT(u.data_criacao, 'yyyy-MM-dd HH:mm:ss') AS usuario_data_criacao,
  u.setor,
  u.pode_transferir,
  'AIR' AS fonte,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao


FROM {{ get_catalogo('silver') }}.stage.vw_air_tbl_vendedor AS v
LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_usuario AS u ON v.usuario = u.codigo
