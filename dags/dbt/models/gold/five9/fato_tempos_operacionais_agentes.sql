WITH legado AS (
  SELECT * FROM {{ source('legado', 'five9_fato_tempos_operacionais_agentes') }}
  WHERE CAST(data_status AS DATE) < '2024-07-01'
),

stg_agent_login_logout AS (
  SELECT * FROM {{ ref('vw_five9_agent_login_logout') }}
  WHERE CAST(data_status AS DATE) >= '2024-07-01'
),

unioned AS (
  SELECT * FROM stg_agent_login_logout

  UNION ALL

  SELECT
    agente,
    TRY_CAST(data_status AS TIMESTAMP) AS data_status,
    status,
    codigo_motivo,
    tempo_condicao_agente,
    tempo_condicao_agente_em_seg,
    TRY_CAST(data_extracao AS TIMESTAMP) AS data_extracao

  FROM legado
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['agente', 'data_status', 'status']) }} AS sk_chamada,
    agente,
    data_status,
    status,
    codigo_motivo,
    tempo_condicao_agente,
    tempo_condicao_agente_em_seg,
    data_extracao

  FROM unioned
)

SELECT *
FROM
  final
