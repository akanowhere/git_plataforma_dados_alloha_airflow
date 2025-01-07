WITH stage_stats AS (
  SELECT * FROM {{ ref('vw_app_loja_stats') }}
)

SELECT *
FROM
  stage_stats
