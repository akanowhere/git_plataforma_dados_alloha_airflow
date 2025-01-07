WITH tbl_produtos_watch AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_produtos_watch') }}
)

SELECT
  id,
  codigo,
  marca,
  produto,
  sku

FROM tbl_produtos_watch
