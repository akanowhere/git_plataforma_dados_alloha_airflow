WITH source AS (
  SELECT *
  FROM {{ source('air_tv', 'tbl_produtos_gigamaistv') }}
)

SELECT
  id,
  marca,
  produto,
  codigo,
  sku

FROM source
