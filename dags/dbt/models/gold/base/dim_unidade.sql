WITH

vw_air_tbl_unidade AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_unidade') }}
),

transformed AS (
  SELECT
    id AS id_unidade,
    DATE_FORMAT(data_criacao, 'yyyy-MM-dd HH:mm:ss') AS data_criacao,
    usuario_criacao,
    DATE_FORMAT(data_alteracao, 'yyyy-MM-dd HH:mm:ss') AS data_alteracao,
    usuario_alteracao,
    excluido,
    sigla,
    nome,
    regional,
    area_distribuicao,
    marca,
    estado,
    cluster,
    'AIR' AS fonte,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM vw_air_tbl_unidade
)

SELECT * FROM transformed;
