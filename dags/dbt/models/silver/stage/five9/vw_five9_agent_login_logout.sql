{% set catalog_schema_table=source('five9_agent_reports', 'agent_login_logout') %}
{% set partition_column="AGENT, TIMESTAMPADD(HOUR, `_timezone_diff`, TO_TIMESTAMP(CONCAT(`DATE`, ' ', `TIME`), 'yyyy/MM/dd HH:mm:ss')), STATE" %}
{% set order_column="_extraction_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    AGENT AS agente,
    TIMESTAMPADD(HOUR, _timezone_diff, TO_TIMESTAMP(CONCAT(DATE, ' ', TIME), 'yyyy/MM/dd HH:mm:ss')) AS data_status,
    STATE AS status,
    NULLIF(REASON_CODE, '') AS codigo_motivo,
    AGENT_STATE_TIME AS tempo_condicao_agente,
    {{ convert_time_to_seconds('AGENT_STATE_TIME') }} AS tempo_condicao_agente_em_seg,
    CAST(_extraction_date AS TIMESTAMP) AS data_extracao

  FROM latest
)

SELECT *
FROM
  transformed
