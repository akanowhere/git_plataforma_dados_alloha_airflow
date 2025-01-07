{{
  config(
    alias = 'dim_event_type'
    )
}}

WITH vw_ecare_event_type AS (
  SELECT
    id,
    key,
    description,
    created_at,
    has_incident,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM {{ ref('vw_ecare_event_type') }}
)

SELECT *
FROM vw_ecare_event_type
