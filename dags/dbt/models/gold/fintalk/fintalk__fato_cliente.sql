{{
  config(
    alias = 'fato_cliente',
    materialized = 'incremental'
    )
}}

SELECT *
FROM {{ ref('vw_fintalk_customers') }}

{% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})

{% endif %}
