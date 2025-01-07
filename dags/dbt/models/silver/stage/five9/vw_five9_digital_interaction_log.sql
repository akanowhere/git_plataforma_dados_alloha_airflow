{% set catalog_schema_table=source('five9_digital_channel_reports', 'digital_interaction_log') %}
{% set partition_column="SESSION_GUID" %}
{% set order_column="_extraction_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    SESSION_GUID AS id_sessao_chamada,
    TIMESTAMPADD(HOUR, _timezone_diff, TO_TIMESTAMP(CONCAT(DATE, ' ', TIME), 'yyyy/MM/dd HH:mm:ss')) AS data_inicio_chamada,
    CAMPAIGN AS campanha,
    NULLIF(NULLIF(AGENT, '[None]'), '') AS agente,
    NULLIF(NULLIF(SKILL, '[Deleted]'), '') AS competencia,
    DISPOSITION AS posicao,
    MEDIA_TYPE AS tipo_midia,
    MEDIA_SUBTYPE AS subtipo_midia,
    CASE WHEN CUSTOMER_NAME IN ('nan', '[None]', '') THEN NULL ELSE CUSTOMER_NAME END AS cliente,
    NULLIF(NULLIF(TO_ADDRESS, 'nan'), '') AS para_endereco,
    CASE WHEN FROM_ADDRESS IN ('nan', '[None]', '') THEN NULL ELSE FROM_ADDRESS END AS do_endereco,
    NULLIF(NULLIF(INTERACTION_TIME, 'nan'), '') AS tempo_interacao,
    NULLIF(NULLIF(CHAT_TIME, 'nan'), '') AS tempo_conversa,
    NULLIF(NULLIF(QUEUE_TIME, 'nan'), '') AS tempo_espera_fila,
    NULLIF(NULLIF(AFTER_CHAT_WORK, 'nan'), '') AS tempo_apos_horario_trabalho,
    CASE WHEN TRANSFERRED_FROM IN ('nan', '[None]', '') THEN NULL ELSE TRANSFERRED_FROM END AS transferida_de,
    CASE WHEN TRANSFERRED_TO IN ('nan', '[None]', '') THEN NULL ELSE TRANSFERRED_TO END AS transferida_para,
    TRY_CAST(TRY_CAST(TRANSFERS AS FLOAT) AS INT) AS transferencias,
    CAST(_extraction_date AS TIMESTAMP) AS data_extracao

  FROM latest
)

SELECT *
FROM
  transformed
