WITH source AS (
  SELECT *
  FROM {{ source('air_internet', 'tbl_aviso_unidade') }}
),

transformed AS (
  SELECT
    id_aviso,
    unidades

  FROM source
)

SELECT *
FROM transformed
