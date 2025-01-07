{{
  config(
    materialized='incremental',
    unique_key='id'
    )
}}

WITH stg_os_notificacao_confirmacao AS (
  SELECT * FROM {{ ref('vw_air_tbl_os_notificacao_confirmacao') }}
)

SELECT *
FROM
  stg_os_notificacao_confirmacao

{% if is_incremental() %}

  WHERE id NOT IN (SELECT id FROM {{ this }})

{% endif %}
