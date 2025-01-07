{{ config(
    materialized='table',
    unique_key='sk_chamada'
) }}

WITH stg_agent_disconnects_first AS (
  SELECT * FROM {{ ref('vw_five9_agent_disconnects_first') }}
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['id_chamada', 'data_inicio_chamada', 'agente']) }} AS sk_chamada,
    id_chamada,
    data_inicio_chamada,
    campanha,
    tipo_chamada,
    agente,
    flg_agente_desconecta_primeiro,
    data_extracao

  FROM stg_agent_disconnects_first
)

SELECT * FROM final

{% if is_incremental() %}

  WHERE sk_chamada NOT IN (SELECT sk_chamada FROM {{ this }})

{% endif %}
