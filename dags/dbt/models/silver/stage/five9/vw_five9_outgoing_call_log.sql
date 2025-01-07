{% set catalog_schema_table=source('five9_call_log_reports', 'outgoing_call_log') %}
{% set partition_column="CALL_ID" %}
{% set order_column="_extraction_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    CALL_ID AS id_chamada,
    TIMESTAMPADD(HOUR, _timezone_diff, TO_TIMESTAMP(CONCAT(DATE, ' ', TIME), 'yyyy/MM/dd HH:mm:ss')) AS data_inicio_chamada,
    CAMPAIGN AS campanha,
    NULLIF(NULLIF(AGENT, '[None]'), '') AS agente,
    NULLIF(NULLIF(DISPOSITION, '[Deleted]'), '') AS posicao,
    NULLIF(NULLIF(DISPOSITION_GROUP_A, 'nan'), '') AS classificacao_grupo_a,
    NULLIF(NULLIF(DISPOSITION_GROUP_B, 'nan'), '') AS classificacao_grupo_b,
    NULLIF(NULLIF(DISPOSITION_GROUP_C, 'nan'), '') AS classificacao_grupo_c,
    NULLIF(NULLIF(CUSTOMER_NAME, 'nan'), '') AS cliente,
    TRY_CAST(TRY_CAST(Contrato AS FLOAT) AS INT) AS contrato,
    NULLIF(NULLIF(`CPF-CNPJ`, 'nan'), '') AS cpf_cnpj,
    CASE
      WHEN ANI LIKE '+55%' THEN REPLACE(ANI, '+55', '')
      ELSE ANI
    END AS origem,
    CASE
      WHEN DNIS LIKE '+55%' THEN REPLACE(DNIS, '+55', '')
      ELSE DNIS
    END AS destino,
    TRY_CAST(TRY_CAST(TRANSFERS AS FLOAT) AS INT) AS transferencias,
    TRY_CAST(TRY_CAST(ABANDONED AS FLOAT) AS INT) AS abandonada,
    CALL_TYPE AS tipo_chamada,
    NULLIF(NULLIF(CALL_TIME, 'nan'), '') AS tempo_chamada,
    NULLIF(NULLIF(IVR_TIME, 'nan'), '') AS tempo_ivr,
    NULLIF(NULLIF(TALK_TIME, 'nan'), '') AS tempo_conversa,
    NULLIF(NULLIF(DIAL_TIME, 'nan'), '') AS tempo_discagem,
    TRY_CAST(TRY_CAST(ID_Fatura AS FLOAT) AS INT) AS id_fatura,
    CASE
      WHEN UPPER(company) IN ('SUMICITY', 'CLICK', 'UNIVOX', 'VIP', 'NIU', 'LIGUE', 'PAMNET') THEN UPPER(company)
      WHEN company ILIKE 'GIGA%' THEN 'GIGA+ FIBRA'
      WHEN company ILIKE 'MOB%' THEN 'MOBWIRE'
      ELSE NULL
    END AS marca,
    CASE
      WHEN UPPER(company) IN ('SUMICITY', 'GIGA+', 'GIGA+ FIBRA', 'CLICK', 'UNIVOX') THEN 'SUMICITY'
      ELSE
        CASE
          WHEN (CUSTOMER_NAME ILIKE '%SUMICIT%' OR CUSTOMER_NAME ILIKE '%GIGA%' OR CUSTOMER_NAME ILIKE '%CLICK%' OR CUSTOMER_NAME LIKE '%UNIVOX%') THEN 'SUMICITY'
          ELSE
            CASE
              WHEN CAMPAIGN ILIKE '%SUMICIT%' THEN 'SUMICITY'
              WHEN CAMPAIGN ILIKE '%MOB%' THEN 'MOB'
              ELSE 'VIP'
            END
        END
    END polo,
    NULLIF(NULLIF(`Custom.MotivoRetencao`, 'nan'), '') AS motivo_retencao_ura,
    TRY_CAST(_extraction_date AS TIMESTAMP) AS data_extracao

  FROM latest
)

SELECT *
FROM
  transformed
