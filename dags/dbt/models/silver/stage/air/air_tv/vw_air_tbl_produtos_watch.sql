WITH source AS (
  SELECT *
  FROM {{ source('air_tv', 'tbl_produtos_watch') }}
)

SELECT
  id,
  marca,
  produto,
  codigo,
  sku

FROM source
