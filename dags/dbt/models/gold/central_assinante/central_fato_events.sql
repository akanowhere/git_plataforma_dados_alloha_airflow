WITH stage_central_events AS (
  SELECT * FROM {{ ref('vw_central_events') }}
)

SELECT *
FROM
  stage_central_events