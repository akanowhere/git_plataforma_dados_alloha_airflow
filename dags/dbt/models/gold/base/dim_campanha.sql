SELECT
  id AS id_campanha,
  CAST(data_criacao AS TIMESTAMP) AS data_criacao,
  CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
  usuario_alteracao,
  excluido,
  codigo,
  nome,
  fideliza,
  ativo,
  b2b,
  pme,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao



FROM {{ get_catalogo('silver') }}.stage.vw_air_tbl_campanha
