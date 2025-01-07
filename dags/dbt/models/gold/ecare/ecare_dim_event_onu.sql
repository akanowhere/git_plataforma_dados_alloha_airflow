{{
  config(
    alias = 'dim_event_onu'
    )
}}

WITH vw_ecare_event_onu AS (
  SELECT
    id,
    event_id,
    contract_id,
    nature_type,
    description,
    status,
    message,
    created_at,
    updated_at,
    event_onu_status,
    onu_id,
    integrated,
    rx_power,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM {{ ref('vw_ecare_event_onu') }}
)

SELECT *
FROM vw_ecare_event_onu
