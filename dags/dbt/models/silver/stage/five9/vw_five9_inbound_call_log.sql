{% set catalog_schema_table=source('five9_call_log_reports', 'inbound_call_log') %}
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
    CASE WHEN AGENT IN ('[Nenhum]', '[None]', '[Deleted]') THEN NULL ELSE NULLIF(AGENT, '') END AS agente,
    CASE WHEN SKILL IN ('[Nenhum]', '[None]', '[Deleted]') THEN NULL ELSE NULLIF(SKILL, '') END AS competencia,
    NULLIF(NULLIF(DISPOSITION, '[Deleted]'), '') AS posicao,
    NULLIF(NULLIF(DISPOSITION_GROUP_A, 'nan'), '') AS classificacao_grupo_a,
    NULLIF(NULLIF(DISPOSITION_GROUP_B, 'nan'), '') AS classificacao_grupo_b,
    NULLIF(NULLIF(DISPOSITION_GROUP_C, 'nan'), '') AS classificacao_grupo_c,
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
    CALL_TYPE AS tipo_chamada,
    NULLIF(NULLIF(CALL_TIME, 'nan'), '') AS tempo_chamada,
    NULLIF(NULLIF(IVR_TIME, 'nan'), '') AS tempo_ivr,
    NULLIF(NULLIF(TALK_TIME, 'nan'), '') AS tempo_conversa,
    NULLIF(NULLIF(QUEUE_WAIT_TIME, 'nan'), '') AS tempo_espera_fila,
    TRY_CAST(TRY_CAST(TRANSFERS AS FLOAT) AS INT) AS transferencias,
    TRY_CAST(TRY_CAST(ABANDONED AS FLOAT) AS INT) AS abandonada,
    CASE WHEN `Custom.Cidade` IN ('OK', 'Unauthorized', 'undefined', 'nan', '') THEN NULL ELSE `Custom.Cidade` END AS cidade,
    CASE WHEN `Custom.Bairro` IN ('undefined', 'nan', '') THEN NULL ELSE `Custom.Bairro` END AS bairro,
    NULLIF(NULLIF(`Custom.Endereço`, 'nan'), '') AS endereco,
    TRY_CAST(TRY_CAST(`Custom.Contrato` AS FLOAT) AS INT) AS contrato,
    NULLIF(NULLIF(`Custom.Massiva`, 'nan'), '') AS massiva,
    COALESCE(
      CASE
        WHEN REGEXP_LIKE(`Alloha.CPF_CNPJ`, '[A-Za-z]') THEN NULL
        ELSE NULLIF(`Alloha.CPF_CNPJ`, '')
      END,
      CASE
        WHEN REGEXP_LIKE(`Custom.CPF`, '[A-Za-z]') THEN NULL
        ELSE NULLIF(`Custom.CPF`, '')
      END
    ) AS cpf_cnpj,
    CASE WHEN `Custom.EmpresaMarca` ILIKE '%GIGA%' THEN 'GIGA+ FIBRA' ELSE UPPER(NULLIF(NULLIF(`Custom.EmpresaMarca`, 'nan'), '')) END AS marca,
    CASE
      WHEN (CAMPAIGN ILIKE '%SUMICI%' OR CAMPAIGN ILIKE '%GIGA%') THEN 'SUMICITY'
      WHEN (CAMPAIGN ILIKE '%MOB%' OR `Custom.EmpresaMarca` ILIKE '%MOB%' OR `Alloha.Polo` ILIKE '%MOB%') THEN 'MOB'
      ELSE 'VIP'
    END polo,
    NULLIF(NULLIF(`Alloha.Retenção`, 'nan'), '') AS motivo_retencao_ura,
    NULLIF(NULLIF(`Alloha.Caminho`, 'nan'), '') AS caminho,
    NULLIF(NULLIF(`Alloha.Regiao`, 'nan'), '') AS regiao,
    NULLIF(NULLIF(`Alloha.Cluster`, 'nan'), '') AS cluster,
    TRY_CAST(TRY_CAST(`Alloha.CSAT_Auto_Pergunta1` AS FLOAT) AS INT) AS csat_auto_pergunta_1,
    TRY_CAST(TRY_CAST(`Alloha.CSAT_Auto_Pergunta2` AS FLOAT) AS INT) AS csat_auto_pergunta_2,
    TRY_CAST(TRY_CAST(`Alloha.Pergunta1_PesquisaIVR` AS FLOAT) AS INT) AS csat_ath_pergunta_1,
    TRY_CAST(TRY_CAST(`Alloha.Pergunta2_PesquisaIVR` AS FLOAT) AS INT) AS csat_ath_pergunta_2,
    COALESCE(
      TRY_CAST(TRY_CAST(`Pesquisa.Pergunta01` AS FLOAT) AS INT),
      TRY_CAST(TRY_CAST(`Alloha.Pergunta1_PesquisaAgente` AS FLOAT) AS INT)
    ) AS pergunta_satisfacao_1,
    COALESCE(
      TRY_CAST(TRY_CAST(`Pesquisa.Pergunta02` AS FLOAT) AS INT),
      TRY_CAST(TRY_CAST(`Alloha.Pergunta2_PesquisaAgente` AS FLOAT) AS INT)
    ) AS pergunta_satisfacao_2,
    COALESCE(
      TRY_CAST(TRY_CAST(`Pesquisa.Pergunta03` AS FLOAT) AS INT),
      TRY_CAST(TRY_CAST(`Alloha.Pergunta3_PesquisaAgente` AS FLOAT) AS INT)
    ) AS pergunta_satisfacao_3,
    CAST(_extraction_date AS TIMESTAMP) AS data_extracao

  FROM latest
)

SELECT *
FROM
  transformed
