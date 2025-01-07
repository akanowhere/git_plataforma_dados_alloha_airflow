{{ config(
    materialized='table',
    unique_key='sk_chamada'
) }}

WITH

stg_inbound_call_log AS (
  SELECT * FROM {{ ref('vw_five9_inbound_call_log') }}
  WHERE CAST(data_inicio_chamada AS DATE) >= '2024-07-01'
),

int_inbound_call_transfer AS (
  SELECT * FROM {{ ref('int_inbound_call_transfer') }}
),

legado AS (
  SELECT * FROM {{ source('legado', 'five9_fato_chamadas_recebidas_voz') }}
  WHERE CAST(data_inicio_chamada AS DATE) < '2024-07-01'
),

call_transfers_log AS (
  SELECT
    call_log.id_chamada,
    call_transfer.id_segmento_chamada,
    call_transfer.data_inicio_chamada AS data_inicio_chamada,
    call_transfer.data_inicio_atendimento,
    call_log.campanha,
    call_transfer.campanha AS campanha_segmento_chamada,
    call_transfer.agente AS agente,
    call_transfer.competencia AS competencia,
    call_transfer.tipo_segmento,
    call_transfer.destino_transferencia,
    call_log.competencia AS competencia_final,
    call_log.posicao,
    call_log.classificacao_grupo_a,
    call_log.classificacao_grupo_b,
    call_log.classificacao_grupo_c,
    call_log.motivo_retencao_ura,
    call_log.tipo_chamada,
    call_log.cpf_cnpj,
    call_log.contrato,
    call_log.origem,
    call_log.destino,
    call_transfer.transferencias AS transferencias,
    call_log.abandonada,
    call_log.massiva,
    call_log.caminho,
    call_log.regiao,
    call_log.cluster,
    call_log.cidade,
    call_log.bairro,
    call_log.endereco,
    call_log.tempo_chamada,
    call_log.tempo_ivr,
    call_transfer.tempo_conversa AS tempo_conversa,
    call_transfer.tempo_espera_fila AS tempo_espera_fila,
    call_transfer.tempo_toque,
    call_transfer.tempo_pos_atendimento,
    call_log.marca,
    call_log.polo,
    call_log.csat_auto_pergunta_1,
    call_log.csat_auto_pergunta_2,
    call_log.csat_ath_pergunta_1,
    call_log.csat_ath_pergunta_2,
    call_log.pergunta_satisfacao_1,
    call_log.pergunta_satisfacao_2,
    call_log.pergunta_satisfacao_3,
    call_log.data_extracao

  FROM stg_inbound_call_log AS call_log
  INNER JOIN int_inbound_call_transfer AS call_transfer ON call_log.id_chamada = call_transfer.id_chamada
),

unioned AS (
  SELECT * FROM call_transfers_log

  UNION ALL

  SELECT
    id_chamada,
    NULL AS id_segmento_chamada,
    data_inicio_chamada,
    NULL AS data_inicio_atendimento,
    campanha,
    NULL AS campanha_segmento_chamada,
    agente,
    competencia,
    NULL AS tipo_segmento,
    NULL AS destino_transferencia,
    competencia AS competencia_final,
    posicao,
    classificacao_grupo_a,
    classificacao_grupo_b,
    classificacao_grupo_c,
    motivo_retencao_ura,
    tipo_chamada,
    cpf_cnpj,
    contrato,
    origem,
    destino,
    0 AS transferencias,
    abandonada,
    massiva,
    caminho,
    regiao,
    cluster,
    cidade,
    bairro,
    endereco,
    tempo_chamada,
    tempo_ivr,
    tempo_conversa,
    tempo_espera_fila,
    NULL AS tempo_toque,
    NULL AS tempo_pos_atendimento,
    marca,
    polo,
    csat_auto_pergunta_1,
    csat_auto_pergunta_2,
    csat_ath_pergunta_1,
    csat_ath_pergunta_2,
    pergunta_satisfacao_1,
    pergunta_satisfacao_2,
    pergunta_satisfacao_3,
    data_extracao

  FROM stg_inbound_call_log

  WHERE id_chamada NOT IN (SELECT DISTINCT id_chamada FROM call_transfers_log)
),

ranked_calls AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_chamada ORDER BY data_inicio_chamada DESC) AS rn
  FROM unioned
),

union_current_and_legacy_calls AS (
  SELECT
    id_chamada,
    id_segmento_chamada,
    data_inicio_chamada,
    data_inicio_atendimento,
    campanha,
    campanha_segmento_chamada,
    agente,
    competencia,
    tipo_segmento,
    destino_transferencia,
    competencia_final,
    CASE WHEN rn = 1 THEN posicao END AS posicao,
    CASE WHEN rn = 1 THEN classificacao_grupo_a END AS classificacao_grupo_a,
    CASE WHEN rn = 1 THEN classificacao_grupo_b END AS classificacao_grupo_b,
    CASE WHEN rn = 1 THEN classificacao_grupo_c END AS classificacao_grupo_c,
    motivo_retencao_ura,
    tipo_chamada,
    cpf_cnpj,
    contrato,
    origem,
    destino,
    transferencias,
    abandonada,
    massiva,
    caminho,
    regiao,
    cluster,
    cidade,
    bairro,
    endereco,
    tempo_chamada,
    tempo_ivr,
    tempo_conversa,
    tempo_espera_fila,
    tempo_toque,
    tempo_pos_atendimento,
    marca,
    polo,
    csat_auto_pergunta_1,
    csat_auto_pergunta_2,
    csat_ath_pergunta_1,
    csat_ath_pergunta_2,
    pergunta_satisfacao_1,
    pergunta_satisfacao_2,
    pergunta_satisfacao_3,
    data_extracao

  FROM ranked_calls

  UNION ALL

  SELECT
    id_chamada,
    NULL AS id_segmento_chamada,
    TRY_CAST(data_inicio_chamada AS TIMESTAMP) AS data_inicio_chamada,
    TRY_CAST(data_inicio_atendimento AS TIMESTAMP) AS data_inicio_atendimento,
    campanha,
    NULL AS campanha_segmento_chamada,
    agente,
    competencia,
    tipo_segmento,
    destino_transferencia,
    competencia_final,
    posicao,
    classificacao_grupo_a,
    classificacao_grupo_b,
    classificacao_grupo_c,
    motivo_retencao_ura,
    tipo_chamada,
    cpf_cnpj,
    TRY_CAST(TRY_CAST(contrato AS FLOAT) AS INT) AS contrato,
    origem,
    destino,
    TRY_CAST(TRY_CAST(transferencias AS FLOAT) AS INT) AS transferencias,
    TRY_CAST(TRY_CAST(abandonada AS FLOAT) AS INT) AS abandonada,
    massiva,
    caminho,
    regiao,
    cluster,
    cidade,
    bairro,
    endereco,
    tempo_chamada,
    tempo_ivr,
    tempo_conversa,
    tempo_espera_fila,
    tempo_toque,
    tempo_pos_atendimento,
    marca,
    polo,
    TRY_CAST(TRY_CAST(csat_auto_pergunta_1 AS FLOAT) AS INT) AS csat_auto_pergunta_1,
    TRY_CAST(TRY_CAST(csat_auto_pergunta_2 AS FLOAT) AS INT) AS csat_auto_pergunta_2,
    NULL AS csat_ath_pergunta_1,
    NULL AS csat_ath_pergunta_2,
    TRY_CAST(TRY_CAST(pergunta_satisfacao_1 AS FLOAT) AS INT) AS pergunta_satisfacao_1,
    TRY_CAST(TRY_CAST(pergunta_satisfacao_2 AS FLOAT) AS INT) AS pergunta_satisfacao_2,
    TRY_CAST(TRY_CAST(pergunta_satisfacao_3 AS FLOAT) AS INT) AS pergunta_satisfacao_3,
    TRY_CAST(data_extracao AS TIMESTAMP) AS data_extracao

  FROM legado
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['id_chamada', 'id_segmento_chamada']) }} AS sk_chamada,
    *
  FROM union_current_and_legacy_calls
)

SELECT *
FROM
  final

{% if is_incremental() %}

  WHERE sk_chamada NOT IN (SELECT sk_chamada FROM {{ this }})

{% endif %}
