WITH stage_ratings AS (
  SELECT * FROM {{ ref('vw_app_loja_ratings') }}
)

SELECT *
FROM
  stage_ratings
