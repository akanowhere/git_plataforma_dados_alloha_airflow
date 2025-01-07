{{
  config(
    alias = 'dim_aviso_unidade'
    )
}}

WITH vw_air_tbl_aviso_unidade AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_aviso_unidade') }}
)

SELECT
  id_aviso,
  unidades,
  CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM vw_air_tbl_aviso_unidade
