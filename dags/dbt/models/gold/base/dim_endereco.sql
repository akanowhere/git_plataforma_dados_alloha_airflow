SELECT
  CAST(e.id AS CHAR(255)) AS id_endereco,
  DATE_FORMAT(e.data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
  CAST(e.usuario_criacao AS CHAR(255)) AS usuario_criacao,
  DATE_FORMAT(e.data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
  CAST(e.usuario_alteracao AS CHAR(255)) AS usuario_alteracao,
  CAST(e.id_cliente AS CHAR(255)) AS id_cliente,
  CAST(e.numero AS CHAR(255)) AS numero,
  CAST(e.complemento AS CHAR(255)) AS complemento,
  CAST(e.referencia AS CHAR(255)) AS referencia,
  CAST(e.latitude AS CHAR(255)) AS latitude,
  CAST(e.longitude AS CHAR(255)) AS longitude,
  CAST(e.unidade AS CHAR(255)) AS unidade,
  CAST(e.logradouro_tipo AS CHAR(255)) AS logradouro_tipo,
  CAST(e.logradouro AS CHAR(255)) AS logradouro,
  CAST(e.bairro AS CHAR(255)) AS bairro,
  CAST(e.cep AS CHAR(255)) AS cep,
  e.id_cidade,
  CAST(e.ftta AS CHAR(255)) AS ftta,
  CAST(ec.nome AS CHAR(255)) AS cidade,
  CAST(ec.estado AS CHAR(255)) AS estado,
  CAST(ec.codigo_ibge AS CHAR(255)) AS codigo_ibge,
  CAST(ec.atendida AS CHAR(255)) AS atendida,
  'AIR' AS fonte,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM
  {{ get_catalogo('silver') }}.stage.vw_air_tbl_endereco AS e

LEFT JOIN
  {{ get_catalogo('silver') }}.stage.vw_air_tbl_endereco_cidade AS ec ON e.id_cidade = ec.id
