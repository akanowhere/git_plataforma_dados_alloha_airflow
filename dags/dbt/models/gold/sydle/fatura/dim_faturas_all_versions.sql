WITH bronze_legado__dim_faturas_mailing AS (
  SELECT *
  FROM {{ source('legado', 'dim_faturas_mailing') }}
),

bronze_sydle__fatura AS (
  SELECT *
  FROM {{ source('legado', 'fatura') }}
),

silver_stage__vw_air_tbl_cliente_fisico AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_cliente_fisico') }}
),

silver_stage__vw_air_tbl_cliente_juridico AS (
  SELECT *
  FROM {{ ref('vw_air_tbl_cliente_juridico') }}
),

all_invoices AS (
    SELECT DISTINCT *
    FROM (
        SELECT
            DISTINCT
            CAST(codigo AS LONG) AS codigo_fatura_sydle,
            CAST(valor_total AS DOUBLE) valor_fatura,
            CAST(codigo_externo AS LONG) AS codigo_contrato_air,
            forma_pagamento,
            CAST(valor_pago AS DOUBLE) AS valor_pago,
            CAST(data_pagamento AS DATE) AS data_pagamento,
            TRY_CAST(data_atualizacao AS TIMESTAMP) AS data_atualizacao,
            statusfatura AS status_fatura,
            CAST(data_vencimento AS DATE) data_vencimento,
            id_legado AS codigo_legado_fatura,
            classificacao,
            CAST(valor_sem_multa_juros AS DOUBLE) AS valor_sem_multa_juros,
            mes_referencia,
            data_criacao,
            COALESCE(t2.id_cliente, t3.id_cliente) AS codigo_cliente_air,
            try_cast(t1.data_extracao as timestamp)
        FROM bronze_sydle__fatura t1
        LEFT JOIN silver_stage__vw_air_tbl_cliente_fisico t2
            ON regexp_replace(t1.documento_cliente, '[.-]', '') = regexp_replace(t2.cpf, '[.-]', '')
        LEFT JOIN silver_stage__vw_air_tbl_cliente_juridico t3
            ON regexp_replace(t1.documento_cliente, '[.-/]', '') = regexp_replace(t3.cnpj, '[.-/]', '')

        WHERE data_atualizacao IS NOT NULL and DATE(data_atualizacao) > '2024-08-01'

        UNION ALL

        SELECT DISTINCT
            codigo_fatura_sydle,
            CAST(valor_fatura AS DOUBLE) valor_fatura,
            contrato_air AS codigo_contrato_air,
            forma_pagamento,
            CAST(valor_pago AS DOUBLE) AS valor_pago,
            CAST(data_pagamento AS DATE) AS data_pagamento,
            TRY_CAST(data_atualizacao AS TIMESTAMP) AS data_atualizacao,
            status_fatura,
            CAST(data_vencimento AS DATE) data_vencimento,
            id_legado_fatura AS codigo_legado_fatura,
            classificacao,
            CAST(valor_sem_multa_juros AS DOUBLE) AS valor_sem_multa_juros,
            mes_referencia,
            data_criacao,
            id_cliente_air AS codigo_cliente_air,
            try_cast(data_extracao as timestamp)
        FROM bronze_legado__dim_faturas_mailing
        WHERE DATE(data_atualizacao) <= '2024-08-01'
    ) tbl_union
),

date_negociacao AS (
    SELECT
        codigo_fatura_sydle,
        data_atualizacao as data_negociacao,
        ROW_NUMBER() OVER(PARTITION BY codigo_fatura_sydle ORDER BY data_atualizacao DESC, data_extracao DESC) AS is_neg
    FROM all_invoices
    WHERE UPPER(status_fatura) LIKE '%NEG%'
),

version_invoices AS (
    SELECT
        ai.codigo_fatura_sydle,
        ai.valor_fatura,
        ai.codigo_contrato_air,
        ai.forma_pagamento,
        ai.valor_pago,
        ai.data_pagamento,
        ai.data_atualizacao,
        ai.status_fatura,
        ai.data_vencimento,
        dn.data_negociacao,
        ai.codigo_legado_fatura,
        ai.classificacao,
        ai.valor_sem_multa_juros,
        ai.mes_referencia,
        ai.data_criacao,
        ai.codigo_cliente_air,
       ROW_NUMBER() OVER(PARTITION BY ai.codigo_fatura_sydle ORDER BY ai.data_atualizacao ASC, ai.data_extracao ASC) AS versao,
        ROW_NUMBER() OVER(PARTITION BY ai.codigo_fatura_sydle ORDER BY ai.data_atualizacao DESC, ai.data_extracao DESC) AS is_last
    FROM all_invoices AS ai
    LEFT JOIN date_negociacao AS dn
        ON ai.codigo_fatura_sydle = dn.codigo_fatura_sydle AND is_neg = 1
),

adjust_flags AS (
    SELECT
        codigo_fatura_sydle,
        valor_fatura,
        codigo_contrato_air,
        forma_pagamento,
        valor_pago,
        data_pagamento,
        data_atualizacao,
        UPPER({{ translate_column('status_fatura') }}) AS status_fatura,
        data_vencimento,
        versao,
        data_negociacao,
        codigo_legado_fatura,
        UPPER({{ translate_column('classificacao') }}) AS classificacao,
        valor_sem_multa_juros,
        mes_referencia,
        data_criacao,
        codigo_cliente_air,
        CASE WHEN is_last > 1 THEN false ELSE true END is_last
    FROM version_invoices
)

SELECT
    *
FROM adjust_flags
