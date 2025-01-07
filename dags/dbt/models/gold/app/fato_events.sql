WITH stage_events AS (
  SELECT * FROM {{ ref('vw_app_events') }}
)

SELECT *
FROM
  stage_events