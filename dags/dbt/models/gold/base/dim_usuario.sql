WITH

vw_air_tbl_usuario AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_usuario') }}
),

transformed AS (
  SELECT
    id,
    codigo,
    nome,
    email,
    senha,
    administrador,
    ativo,
    DATE_FORMAT(data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
    DATE_FORMAT(data_validacao, 'yyyy-MM-dd HH:mm:ss') AS data_validacao,
    token_validacao,
    url_foto,
    setor,
    pode_transferir

  FROM vw_air_tbl_usuario
)

SELECT *
FROM
  transformed
