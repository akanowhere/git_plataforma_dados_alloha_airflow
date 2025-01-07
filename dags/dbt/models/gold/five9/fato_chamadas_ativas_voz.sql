{{ config(
    materialized='table',
    unique_key='sk_chamada'
) }}

WITH legado AS (
  SELECT * FROM {{ source('legado', 'five9_fato_chamadas_ativas_voz') }}
  WHERE CAST(data_inicio_chamada AS DATE) < '2024-07-01'
),

stg_outgoing_call_log AS (
  SELECT * FROM {{ ref('vw_five9_outgoing_call_log') }}
  WHERE CAST(data_inicio_chamada AS DATE) >= '2024-07-01'
),

unioned AS (
  SELECT * FROM stg_outgoing_call_log

  UNION ALL

  SELECT
    id_chamada,
    TRY_CAST(data_inicio_chamada AS TIMESTAMP) AS data_inicio_chamada,
    campanha,
    agente,
    posicao,
    classificacao_grupo_a,
    classificacao_grupo_b,
    classificacao_grupo_c,
    cliente,
    TRY_CAST(TRY_CAST(contrato AS FLOAT) AS INT) AS contrato,
    cpf_cnpj,
    origem,
    destino,
    TRY_CAST(TRY_CAST(transferencias AS FLOAT) AS INT) AS transferencias,
    TRY_CAST(TRY_CAST(abandonada AS FLOAT) AS INT) AS abandonada,
    tipo_chamada,
    tempo_chamada,
    tempo_ivr,
    tempo_conversa,
    tempo_discagem,
    TRY_CAST(TRY_CAST(id_fatura AS FLOAT) AS INT) AS id_fatura,
    marca,
    polo,
    motivo_retencao_ura,
    TRY_CAST(data_extracao AS TIMESTAMP) AS data_extracao

  FROM legado
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['id_chamada']) }} AS sk_chamada,
    id_chamada,
    data_inicio_chamada,
    campanha,
    agente,
    posicao,
    classificacao_grupo_a,
    classificacao_grupo_b,
    classificacao_grupo_c,
    cliente,
    contrato,
    cpf_cnpj,
    origem,
    destino,
    transferencias,
    abandonada,
    tipo_chamada,
    tempo_chamada,
    tempo_ivr,
    tempo_conversa,
    tempo_discagem,
    id_fatura,
    marca,
    polo,
    motivo_retencao_ura,
    data_extracao

  FROM unioned
)

SELECT * FROM final

{% if is_incremental() %}

  WHERE sk_chamada NOT IN (SELECT sk_chamada FROM {{ this }})

{% endif %}
