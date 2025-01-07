{{
  config(
    alias = 'fato_csat',
    materialized = 'incremental'
    )
}}

SELECT *
FROM {{ ref('vw_fintalk_csat') }}

{% if is_incremental() %}

  WHERE data_hora >= (SELECT MAX(data_hora) FROM {{ this }})

{% endif %}
