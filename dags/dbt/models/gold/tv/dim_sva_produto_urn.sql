WITH tbl_sva_produto_urn AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_sva_produto_urn') }}
)

SELECT
  id,
  data_criacao,
  usuario_criacao,
  data_alteracao,
  usuario_alteracao,
  excluido,
  id_urn,
  codigo,
  descricao

FROM tbl_sva_produto_urn
