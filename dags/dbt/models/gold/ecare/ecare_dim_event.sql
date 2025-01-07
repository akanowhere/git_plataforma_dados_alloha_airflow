{{
  config(
    alias = 'dim_event'
    )
}}

WITH vw_ecare_event AS (

  SELECT
    id,
    event_type_id,
    {# event_identifier, #}
    created_at,
    incident_id,
    origin,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM {{ ref('vw_ecare_event') }}
)

SELECT *
FROM vw_ecare_event
