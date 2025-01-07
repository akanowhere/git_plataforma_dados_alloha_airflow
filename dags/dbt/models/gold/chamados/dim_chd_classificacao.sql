WITH vw_air_tbl_chd_classificacao AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_classificacao') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  codigo,
  nome,
  tipo,
  ativo

FROM vw_air_tbl_chd_classificacao
