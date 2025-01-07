{{
  config(
    alias = 'dim_onu'
    )
}}

WITH vw_ecare_onu AS (
  SELECT
    id,
    contract_id,
    customer_id,
    serial,
    pon,
    slot,
    olt,
    status,
    telephone,
    neighborhood,
    unity,
    region,
    manufacture,
    onu_index,
    rx_power,
    date_status,
    created_at,
    updated_at,
    olt_ip,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM {{ ref('vw_ecare_onu') }}
)

SELECT *
FROM vw_ecare_onu
