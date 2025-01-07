{% set catalog_schema_table=source('five9_call_segment_reports', 'inbound_call_segment') %}
{% set partition_column="CALL_SEGMENT_ID" %}
{% set order_column="_extraction_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    CALL_SEGMENT_ID AS id_segmento_chamada,
    CALL_ID AS id_chamada,
    TIMESTAMPADD(HOUR, _timezone_diff, TO_TIMESTAMP(CONCAT(DATE, ' ', TIME), 'yyyy/MM/dd HH:mm:ss')) AS data_inicio_chamada,
    CAMPAIGN AS campanha,
    CASE WHEN SKILL IN ('[Nenhum]', '[None]', '[Deleted]') THEN NULL ELSE NULLIF(SKILL, '') END AS competencia,
    CASE
      WHEN TRANSLATE(SUBSTRING(ANI FROM CASE WHEN ANI LIKE '+%' THEN 2 ELSE 1 END), '0123456789', '') <> '' THEN NULL
      WHEN ANI LIKE '+55%' THEN SUBSTRING(ANI FROM 4)
      WHEN ANI LIKE '55%' THEN SUBSTRING(ANI FROM 3)
      ELSE ANI
    END AS origem,
    CASE
      WHEN TRANSLATE(SUBSTRING(DNIS FROM CASE WHEN DNIS LIKE '+%' THEN 2 ELSE 1 END), '0123456789', '') <> '' THEN NULL
      WHEN DNIS LIKE '+55%' THEN SUBSTRING(DNIS FROM 4)
      WHEN DNIS LIKE '55%' THEN SUBSTRING(DNIS FROM 3)
      ELSE DNIS
    END AS destino,
    CALLED_PARTY AS segmento,
    CALLING_PARTY AS chamador,
    RESULT AS resultado,
    NULLIF(NULLIF(DISPOSITION, '[Deleted]'), '') AS posicao,
    SEGMENT_TYPE AS tipo_segmento,
    SEGMENT_TIME AS tempo_segmento,
    CALL_TYPE AS tipo_chamada,
    NULLIF(NULLIF(CALL_TIME, 'nan'), '') AS tempo_chamada,
    NULLIF(NULLIF(IVR_TIME, 'nan'), '') AS tempo_ivr,
    NULLIF(NULLIF(TALK_TIME, 'nan'), '') AS tempo_conversa,
    NULLIF(NULLIF(QUEUE_WAIT_TIME, 'nan'), '') AS tempo_espera_fila,
    NULLIF(NULLIF(RING_TIME, 'nan'), '') AS tempo_toque,
    AFTER_CALL_WORK_TIME AS tempo_pos_atendimento,
    TRANSFERS AS transferencias,
    CAST(_extraction_date AS TIMESTAMP) AS data_extracao

  FROM latest
)

SELECT *
FROM
  transformed
