{{
  config(
    alias = 'fato_mudanca_plano_cliente',
    materialized = 'incremental'
    )
}}

SELECT *
FROM {{ ref('vw_fintalk_mudanca_plano_cliente') }}

{% if is_incremental() %}

  WHERE data_hora >= (SELECT MAX(data_hora) FROM {{ this }})

{% endif %}
