{% set catalog_schema_table=source('five9_agent_reports', 'agent_disconnects_first') %}
{% set partition_column="CALL_ID, TIMESTAMPADD(HOUR, `_timezone_diff`, TO_TIMESTAMP(CONCAT(`DATE`, ' ', `TIME`), 'yyyy/MM/dd HH:mm:ss')), AGENT_EMAIL" %}
{% set order_column="_extraction_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    CALL_ID AS id_chamada,
    TIMESTAMPADD(HOUR, _timezone_diff, TO_TIMESTAMP(CONCAT(DATE, ' ', TIME), 'yyyy/MM/dd HH:mm:ss')) AS data_inicio_chamada,
    CAMPAIGN AS campanha,
    CALL_TYPE AS tipo_chamada,
    AGENT_EMAIL AS agente,
    TRY_CAST(AGENT_DISCONNECTS_FIRST AS INT) AS flg_agente_desconecta_primeiro,
    CAST(_extraction_date AS TIMESTAMP) AS data_extracao

  FROM latest
)

SELECT *
FROM
  transformed
