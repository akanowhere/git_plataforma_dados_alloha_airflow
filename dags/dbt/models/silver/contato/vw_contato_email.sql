SELECT
  id,
  data_criacao,
  data_alteracao,
  excluido,
  id_cliente,
  tipo,
  contato,
  ativo,
  confirmado,
  status,
  favorito,
  canal_confirmacao,
  data_alter_contato,
  data_confirmacao,
  origem
FROM {{ get_catalogo('silver') }}.stage.vw_air_tbl_cliente_contato
WHERE tipo = 'EMAIL'
