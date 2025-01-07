WITH vw_air_tbl_chd_fila AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_fila') }}
)

SELECT
  id AS id_fila,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  codigo,
  nome,
  ativo

FROM vw_air_tbl_chd_fila
