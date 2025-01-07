WITH stg_inbound_call_segment AS (
  SELECT * FROM {{ ref('vw_five9_inbound_call_segment') }}
  WHERE CAST(data_inicio_chamada AS DATE) >= '2024-07-01'
),

columns_enrichment AS (
  SELECT
    id_chamada,
    id_segmento_chamada,
    COALESCE(
      LAG(data_inicio_chamada) OVER (PARTITION BY id_chamada ORDER BY id_segmento_chamada),
      data_inicio_chamada
    ) AS data_inicio_chamada,
    data_inicio_chamada AS data_inicio_atendimento,
    campanha,
    CASE WHEN segmento LIKE '%@%' THEN segmento END AS agente,
    tipo_segmento,
    FIRST_VALUE(tempo_chamada)
      OVER (
        PARTITION BY id_chamada
        ORDER BY
          id_segmento_chamada
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    AS tempo_chamada,
    tempo_conversa,
    LAG(tempo_espera_fila) OVER (PARTITION BY id_chamada ORDER BY id_segmento_chamada) AS tempo_espera_fila,
    tempo_toque,
    tempo_pos_atendimento,
    LAG(segmento) OVER (PARTITION BY id_chamada ORDER BY id_segmento_chamada) AS segmento

  FROM stg_inbound_call_segment
),

skill_treatment_base AS (
  SELECT
    *,
    FIRST_VALUE(
      CASE
        WHEN segmento NOT LIKE '%@%'
          THEN segmento
      END
    )
      OVER (
        PARTITION BY
          id_chamada,
          agente
        ORDER BY
          id_segmento_chamada
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    AS competencia

  FROM columns_enrichment

  WHERE agente IS NOT NULL
),

filtered_for_transfer AS (
  -- Aqui, filtramos para ignorar "Silent Monitoring" nas transferências
  SELECT
    *
  FROM skill_treatment_base
  WHERE tipo_segmento != 'Silent Monitoring'
),

transfer_identification_base AS (
  SELECT
    *,
    CASE
      WHEN
        COUNT(id_chamada) OVER (PARTITION BY id_chamada) = 1 OR
        ROW_NUMBER() OVER (PARTITION BY id_chamada ORDER BY id_segmento_chamada DESC) = 1 OR
        agente = LEAD(agente) OVER (PARTITION BY id_chamada ORDER BY id_segmento_chamada)
        THEN 0
      ELSE 1
    END AS transferencias

  FROM filtered_for_transfer
),

human_transfer_base_final AS (
  SELECT
    stb.*,
    COALESCE(tib.transferencias, 0) AS transferencias,
    CASE
      WHEN tib.transferencias = 1 THEN
        LEAD(tib.competencia) OVER (PARTITION BY tib.id_chamada ORDER BY tib.id_segmento_chamada)
    END AS destino_transferencia

  FROM skill_treatment_base AS stb
  LEFT JOIN transfer_identification_base AS tib ON stb.id_segmento_chamada = tib.id_segmento_chamada
)

SELECT *
FROM
  human_transfer_base_final
