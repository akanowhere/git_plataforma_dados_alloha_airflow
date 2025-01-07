{{
  config(
    alias = 'dim_contrato_onu_aviso'
    )
}}

WITH vw_air_tbl_contrato_onu_aviso AS (
  SELECT
    id,
    aviso_data,
    aviso_descricao,
    onu_serial,
    onu_olt,
    onu_slot,
    onu_pon,
    onu_index,
    onu_rx_power,
    onu_descricao,
    id_contrato,
    id_problema,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

  FROM {{ ref('vw_air_tbl_contrato_onu_aviso') }}
)

SELECT *
FROM vw_air_tbl_contrato_onu_aviso
