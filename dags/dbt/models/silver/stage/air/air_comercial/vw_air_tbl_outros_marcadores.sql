WITH source AS (
  SELECT *
  FROM {{ source("air_comercial", "tbl_outros_marcadores") }}
),

transformed AS (
  SELECT
    id,
    id_cliente,
    marcador

  FROM source
)

SELECT *
FROM transformed

