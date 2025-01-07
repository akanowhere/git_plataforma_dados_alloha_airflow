WITH stage_sessions AS (
  SELECT * FROM {{ ref('vw_app_sessions') }}
)

SELECT *
FROM
  stage_sessions