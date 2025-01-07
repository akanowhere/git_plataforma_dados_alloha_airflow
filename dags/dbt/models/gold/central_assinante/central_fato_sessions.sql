WITH stage_central_sessions AS (
  SELECT * FROM {{ ref('vw_central_sessions') }}
)

SELECT *
FROM
  stage_central_sessions