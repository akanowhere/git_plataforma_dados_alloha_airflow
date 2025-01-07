{% set catalog_schema_table=source('five9_digital_channel_reports', 'digital_interaction_variables') %}
{% set partition_column="CALL_ID" %}
{% set order_column="_extraction_date" %}

WITH latest AS (
  {{ dynamic_table_query(catalog_schema_table, partition_column, order_column) }}
),

transformed AS (
  SELECT
    `Custom.Guia_da_Sessão` AS id_sessao_chamada,
    CALL_ID AS id_chamada,
    TIMESTAMPADD(HOUR, _timezone_diff, TO_TIMESTAMP(CONCAT(DATE, ' ', TIME), 'yyyy/MM/dd HH:mm:ss')) AS data_inicio_chamada,
    CAMPAIGN AS campanha,
    NULLIF(NULLIF(REPLACE(`Custom.CEP`, '.0', ''), 'nan'), '') AS cep,
    NULLIF(NULLIF(`Custom.Cidade`, 'nan'), '') AS cidade,
    NULLIF(NULLIF(`Custom.Bairro`, 'nan'), '') AS bairro,
    NULLIF(NULLIF(`Alloha.NumeroChat`, 'nan'), '') AS numero_chat,
    COALESCE(
      TRY_CAST(TRY_CAST(`Alloha.Contrato` AS FLOAT) AS INT),
      TRY_CAST(TRY_CAST(`Custom.Contrato` AS FLOAT) AS INT)
    ) AS contrato,
    NULLIF(NULLIF(`Alloha.CPF_CNPJ`, 'nan'), '') AS cpf_cnpj,
    NULLIF(NULLIF(`Custom.CPF`, 'nan'), '') AS `custom.cpf`,
    COALESCE(
      CASE
        WHEN UPPER(`Custom.EmpresaMarca`) IN ('SUMICITY', 'CLICK', 'UNIVOX', 'VIP', 'NIU', 'LIGUE', 'PAMNET') THEN UPPER(`Custom.EmpresaMarca`)
        WHEN `Custom.EmpresaMarca` ILIKE 'GIGA%' THEN 'GIGA+ FIBRA'
        WHEN `Custom.EmpresaMarca` ILIKE 'MOB%' THEN 'MOBWIRE'
        ELSE NULL
      END,
      CASE
        WHEN UPPER(`Alloha.Marca`) IN ('SUMICITY', 'CLICK', 'UNIVOX', 'VIP', 'NIU', 'LIGUE', 'PAMNET') THEN UPPER(`Alloha.Marca`)
        WHEN `Alloha.Marca` ILIKE 'GIGA%' THEN 'GIGA+ FIBRA'
        WHEN `Alloha.Marca` ILIKE 'MOB%' THEN 'MOBWIRE'
        ELSE NULL
      END
    ) AS marca,
    COALESCE(
      CASE
        WHEN UPPER(`Custom.EmpresaMarca`) IN ('SUMICITY', 'CLICK', 'UNIVOX') THEN 'SUMICITY'
        WHEN `Custom.EmpresaMarca` ILIKE 'GIGA%' THEN 'SUMICITY'
        WHEN UPPER(`Custom.EmpresaMarca`) IN ('VIP', 'LIGUE', 'NIU') THEN 'VIP'
        WHEN `Custom.EmpresaMarca` ILIKE 'MOB%' THEN 'MOB'
        ELSE NULL
      END,
      CASE
        WHEN UPPER(`Alloha.Marca`) IN ('SUMICITY', 'CLICK', 'UNIVOX') THEN 'SUMICITY'
        WHEN `Alloha.Marca` ILIKE 'GIGA%' THEN 'SUMICITY'
        WHEN UPPER(`Alloha.Marca`) IN ('VIP', 'LIGUE', 'NIU') THEN 'VIP'
        WHEN `Alloha.Marca` ILIKE 'MOB%' THEN 'MOB'
        ELSE NULL
      END
    ) AS polo,
    NULLIF(NULLIF(`Alloha.Retenção`, 'nan'), '') AS motivo_retencao_ura,
    NULLIF(NULLIF(`Alloha.Pergunta1_PesquisaAgente`, 'nan'), '') AS pergunta_satisfacao_1,
    NULLIF(NULLIF(`Alloha.Pergunta2_PesquisaAgente`, 'nan'), '') AS pergunta_satisfacao_2,
    NULLIF(NULLIF(`Alloha.Pergunta3_PesquisaAgente`, 'nan'), '') AS pergunta_satisfacao_3,
    CAST(_extraction_date AS TIMESTAMP) AS data_extracao

  FROM latest
)

SELECT *
FROM
  transformed
