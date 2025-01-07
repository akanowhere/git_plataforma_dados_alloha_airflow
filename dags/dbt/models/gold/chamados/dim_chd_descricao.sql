WITH vw_air_tbl_chd_descricao AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_chd_descricao') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  id_chamado,
  id_fila,
  texto

FROM vw_air_tbl_chd_descricao
