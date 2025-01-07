{{
  config(
    alias = 'dim_incident'
    )
}}

WITH vw_ecare_incident AS (
  SELECT
    id,
    incident_level_id,
    incident_type_id,
    status,
    responsible_team,
    {# protocol, #}
    active_ura,
    quantity_onu_affected,
    pon,
    slot,
    olt,
    description,
    problem_identifier,
    affected_tv,
    affected_internet,
    affected_phone,
    finalized_reason,
    incident_origin,
    {# integration_identifier, #}
    estimated_resolution_at,
    next_interaction_at,
    finalized_at,
    updated_at,
    created_at,
    message,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM {{ ref('vw_ecare_incident') }}
)

SELECT *
FROM vw_ecare_incident
