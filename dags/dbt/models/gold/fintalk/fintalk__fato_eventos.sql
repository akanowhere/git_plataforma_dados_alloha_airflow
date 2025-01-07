{{
  config(
    alias = 'fato_eventos',
    materialized = 'incremental'
    )
}}

SELECT *
FROM {{ ref('vw_fintalk_events') }}

{% if is_incremental() %}

  WHERE created_at >= (SELECT MAX(created_at) FROM {{ this }})

{% endif %}
