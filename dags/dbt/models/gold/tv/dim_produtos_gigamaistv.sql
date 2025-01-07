WITH tbl_produtos_gigamaistv AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_produtos_gigamaistv') }}
)

SELECT
  id,
  marca,
  produto,
  codigo,
  sku

FROM tbl_produtos_gigamaistv
