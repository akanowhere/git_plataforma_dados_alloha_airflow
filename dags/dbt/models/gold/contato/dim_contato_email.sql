SELECT
  id,
  CAST(data_criacao AS TIMESTAMP) AS data_criacao,
  CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
  CASE WHEN excluido = 'F' THEN '0' ELSE '1' END AS excluido,
  id_cliente,
  tipo,
  contato,
  CASE WHEN ativo = 'F' THEN '0' ELSE '1' END AS ativo,
  CASE WHEN confirmado = 'F' THEN '0' ELSE '1' END AS confirmado,
  status,
  CASE WHEN favorito = 'F' THEN '0' ELSE '1' END AS favorito,
  canal_confirmacao,
  CAST(data_alter_contato AS TIMESTAMP) AS data_alter_contato,
  CAST(data_confirmacao AS TIMESTAMP) AS data_confirmacao,
  'AIR' AS origem,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM {{ get_catalogo('silver') }}.stage_contato.vw_contato_email -- o schema não pode ser contato, precisa ser stage_contato
