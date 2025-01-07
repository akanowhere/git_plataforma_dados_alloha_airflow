{{ config(
    materialized='table',
    unique_key='sk_chamada'
) }}

WITH legado AS  (
  SELECT * FROM {{ source('legado', 'five9_fato_chamadas_chat') }}
  WHERE CAST(data_inicio_chamada AS DATE) < '2024-09-01'
),

stg_digital_interaction_log AS (
  SELECT * FROM {{ ref('vw_five9_digital_interaction_log') }}
  WHERE CAST(data_inicio_chamada AS DATE) >= '2024-09-01'
),

stg_digital_interaction_variables AS (
  SELECT * FROM {{ ref('vw_five9_digital_interaction_variables') }}
  WHERE CAST(data_inicio_chamada AS DATE) >= '2024-09-01'
),

joined AS (
  SELECT
    chat_log.id_sessao_chamada,
    chat_variables.id_chamada,
    chat_log.data_inicio_chamada,
    chat_log.campanha,
    chat_log.agente,
    chat_log.competencia,
    chat_log.posicao,
    chat_log.tipo_midia,
    chat_log.subtipo_midia,
    chat_variables.motivo_retencao_ura,
    chat_log.cliente,
    chat_variables.contrato,
    chat_variables.cpf_cnpj,
    chat_variables.cep,
    chat_variables.cidade,
    chat_variables.bairro,
    chat_log.do_endereco AS origem,
    chat_log.para_endereco AS destino,
    chat_log.tempo_interacao,
    chat_log.tempo_conversa,
    chat_log.tempo_espera_fila,
    chat_log.tempo_apos_horario_trabalho,
    chat_log.transferida_de,
    chat_log.transferida_para,
    chat_log.transferencias,
    chat_variables.marca,
    chat_variables.polo,
    chat_log.data_extracao

  FROM stg_digital_interaction_log AS chat_log
  LEFT JOIN stg_digital_interaction_variables AS chat_variables ON chat_log.id_sessao_chamada = chat_variables.id_sessao_chamada
),

unioned AS (
  SELECT * FROM joined

  UNION ALL

  SELECT
    id_sessao_chamada,
    id_chamada,
    TRY_CAST(data_inicio_chamada AS TIMESTAMP) AS data_inicio_chamada,
    campanha,
    agente,
    competencia,
    posicao,
    tipo_midia,
    subtipo_midia,
    motivo_retencao_ura,
    cliente,
    TRY_CAST(TRY_CAST(contrato AS FLOAT) AS INT) AS contrato,
    cpf_cnpj,
    cep,
    cidade,
    bairro,
    origem,
    destino,
    tempo_interacao,
    tempo_conversa,
    tempo_espera_fila,
    tempo_apos_horario_trabalho,
    transferida_de,
    transferida_para,
    TRY_CAST(TRY_CAST(transferencias AS FLOAT) AS INT) AS transferencias,
    marca,
    polo,
    TRY_CAST(data_extracao AS TIMESTAMP) AS data_extracao

  FROM legado
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['id_sessao_chamada']) }} AS sk_chamada,
    id_sessao_chamada,
    id_chamada,
    data_inicio_chamada,
    campanha,
    agente,
    competencia,
    posicao,
    tipo_midia,
    subtipo_midia,
    motivo_retencao_ura,
    cliente,
    contrato,
    cpf_cnpj,
    cep,
    cidade,
    bairro,
    origem,
    destino,
    tempo_interacao,
    tempo_conversa,
    tempo_espera_fila,
    tempo_apos_horario_trabalho,
    transferida_de,
    transferida_para,
    transferencias,
    marca,
    polo,
    data_extracao

  FROM unioned
)

SELECT * FROM final

{% if is_incremental() %}

  WHERE sk_chamada NOT IN (SELECT sk_chamada FROM {{ this }})

{% endif %}