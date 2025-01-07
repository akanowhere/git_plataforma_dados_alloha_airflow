WITH tbl_sva AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_sva') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  nome,
  descricao

FROM tbl_sva
