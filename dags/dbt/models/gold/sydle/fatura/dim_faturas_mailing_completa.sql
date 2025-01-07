WITH bronze_legado__dim_faturas_mailing AS (
  SELECT *
  FROM {{ source('legado', 'dim_faturas_mailing') }}
),

silver_stage__vw_sydle_tbl_fatura AS (
  SELECT *
  FROM {{ ref('vw_sydle_tbl_fatura') }}
),

silver_stage__vw_maling_tbl_fatura AS (
  SELECT *
  FROM {{ ref('vw_mailing_tbl_fatura') }}
),

gold_sydle__dim_faturas_mailing AS (
  SELECT *
  FROM {{ ref('dim_faturas_mailing') }}
),

gold_base__dim_cliente AS (
  SELECT *
  FROM {{ ref('dim_cliente') }}
),

gold_base__dim_contrato AS (
  SELECT *
  FROM {{ ref('dim_contrato') }}
),

gold_base__dim_unidade AS (
  SELECT *
  FROM {{ ref('dim_unidade') }}
),

cte_versao_faturas_anteriores AS (
  SELECT
    id_fatura,
    COUNT(*) AS num
  FROM (
    SELECT id_fatura
    FROM bronze_sydle__fatura
    UNION ALL
    SELECT id_fatura
    FROM gold_sydle__dim_faturas_mailing
  ) a
  GROUP BY id_fatura
),

cte_clientes_air AS (
  SELECT DISTINCT
    cli.id_cliente,
    CASE
      WHEN cli.cnpj IS NULL THEN REPLACE(REPLACE(cli.cpf, '.', ''), '-', '')
      ELSE REPLACE(REPLACE(REPLACE(cli.cnpj, '.', ''), '-', ''), '/', '')
    END AS cpf_cnpj
  FROM gold_base__dim_cliente AS cli
  WHERE cli.fonte = 'AIR'
),

cte_contratos_air AS (
  SELECT DISTINCT
    ctr.id_contrato,
    ctr.id_cliente,
    ctr.unidade_atendimento,
    uni.marca
  FROM gold_base__dim_contrato AS ctr
  LEFT JOIN gold_base__dim_unidade AS uni ON ctr.unidade_atendimento = uni.sigla
),

cte_faturas_mailing AS (
  SELECT
    id_fatura,
    id_cliente AS id_cliente_sydle,
    codigo AS codigo_fatura_sydle,
    CASE
      WHEN LOWER(codigo_externo) LIKE '%[a-z]%' THEN NULL
      ELSE REPLACE(REPLACE(codigo_externo, CHAR(13), ''), CHAR(10), '')
    END AS contrato_air,
    cliente,
    CASE
      WHEN LEN(tipo_documento_cliente) <= 4 THEN tipo_documento_cliente
      ELSE NULL
    END AS tipo_documento,
    CASE
      WHEN LEN(documento_cliente) <= 18 THEN documento_cliente
      ELSE NULL
    END AS cpf_cnpj,
    mes_referencia,
    REPLACE(SUBSTRING(data_criacao, 0, 20), 'T', ' ') AS data_criacao,
    SUBSTRING(data_vencimento, 0, 11) AS data_vencimento,
    SUBSTRING(data_pagamento, 0, 11) AS data_pagamento,
    REPLACE(SUBSTRING(data_atualizacao, 0, 20), 'T', ' ') AS data_atualizacao,
    classificacao,
    forma_pagamento,
    beneficiario,
    statusfatura AS status_fatura,
    valor_total AS valor_fatura,
    valor_sem_multa_juros,
    valor_pago,
    data_extracao,
    id_legado,
    data_extracao_db
  FROM silver_stage__vw_sydle_tbl_fatura
),

cte_add_cliente_air_nas_faturas AS (
  SELECT DISTINCT
    id_fatura,
    id_cliente_sydle,
    codigo_fatura_sydle,
    clientes.id_cliente AS id_cliente_air,
    faturas.contrato_air,
    cliente,
    tipo_documento,
    faturas.cpf_cnpj,
    mes_referencia,
    data_criacao,
    data_vencimento,
    data_pagamento,
    data_atualizacao,
    classificacao,
    forma_pagamento,
    beneficiario,
    status_fatura,
    valor_fatura,
    valor_sem_multa_juros,
    valor_pago,
    faturas.data_extracao,
    faturas.id_legado,
    faturas.data_extracao_db
  FROM cte_faturas_mailing AS faturas
  LEFT JOIN cte_clientes_air AS clientes ON faturas.cpf_cnpj = clientes.cpf_cnpj
),

cte_add_marca_nas_faturas AS (
  SELECT DISTINCT
    id_fatura,
    id_cliente_sydle,
    faturas.codigo_fatura_sydle,
    id_cliente_air,
    faturas.contrato_air,
    cliente,
    tipo_documento,
    faturas.cpf_cnpj,
    mes_referencia,
    data_criacao,
    data_vencimento,
    data_pagamento,
    data_atualizacao,
    classificacao,
    forma_pagamento,
    beneficiario,
    status_fatura,
    valor_fatura,
    valor_sem_multa_juros,
    valor_pago,
    faturas.data_extracao,
    faturas.id_legado,
    contratos.unidade_atendimento,
    contratos.marca,
    faturas.data_extracao_db,
    neg.data_negociacao,
    CASE
      WHEN contratos.marca IN ('CLICK', 'UNIVOX')
        AND faturas.id_legado IS NOT NULL
        AND b.codigo_fatura IS NULL
        THEN '0'
      ELSE '1'
    END AS fat_cobranca
  FROM cte_add_cliente_air_nas_faturas AS faturas
  LEFT JOIN cte_contratos_air AS contratos ON faturas.id_cliente_air = contratos.id_cliente AND faturas.contrato_air = contratos.id_contrato
  LEFT JOIN (
    SELECT
      codigo_fatura_sydle,
      CAST(MIN(data_atualizacao) AS DATE) AS data_negociacao
    FROM gold_sydle__dim_faturas_mailing
    WHERE status_fatura = 'Negociada com o cliente'
    GROUP BY codigo_fatura_sydle
  ) AS neg ON faturas.codigo_fatura_sydle = neg.codigo_fatura_sydle
  LEFT JOIN (
    SELECT DISTINCT
      codigo_fatura,
      marca
    FROM silver_stage__vw_maling_tbl_fatura
    WHERE marca IN ('CLICK', 'UNIVOX')
  ) AS b ON contratos.marca = b.marca
  AND contratos.marca IN ('CLICK', 'UNIVOX')
  AND CAST(b.codigo_fatura AS VARCHAR(255)) = SUBSTRING(faturas.id_legado, (CHARINDEX('_', faturas.id_legado) + 1), LEN(faturas.id_legado))
)

SELECT DISTINCT
  fat.id_fatura,
  fat_anteriores.num AS versao,
  fat.id_cliente_sydle,
  fat.codigo_fatura_sydle,
  fat.id_cliente_air,
  fat.contrato_air,
  fat.cliente,
  fat.tipo_documento,
  fat.cpf_cnpj,
  fat.mes_referencia,
  fat.data_criacao,
  fat.data_vencimento,
  fat.data_pagamento,
  fat.data_atualizacao,
  fat.classificacao,
  fat.forma_pagamento,
  fat.beneficiario,
  fat.status_fatura,
  fat.valor_fatura,
  fat.valor_sem_multa_juros,
  fat.valor_pago,
  fat.data_extracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP) AS data_referencia,
  fat.id_legado AS id_legado_fatura,
  fat.unidade_atendimento,
  fat.marca,
  fat.data_extracao_db,
  fat.data_negociacao,
  fat.fat_cobranca
FROM cte_add_marca_nas_faturas AS fat
LEFT JOIN cte_versao_faturas_anteriores AS fat_anteriores ON fat.id_fatura = fat_anteriores.id_fatura
WHERE fat.id_fatura IS NOT NULL
