{{ config(
    materialized='ephemeral'
) }}


{% set dates = get_dates(['today'],None, 'faturas_elegiveis') %}


WITH lastest_version_faturas AS (
    SELECT 
        *
    FROM  {{ ref('tmp_faturas_all') }}
    WHERE
        status_fatura IN ('EMITIDA', 'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO', 'AGUARDANDO BAIXA EM SISTEMA EXTERNO')
        AND valor_fatura >= 30
        AND data_vencimento < '{{ dates.today }}'
)

, lastest_version_abertas AS (
    SELECT 
        *
    FROM lastest_version_faturas
)
, lastest_version_faturas_validas_mailing AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY codigo_contrato_air ORDER BY data_vencimento ASC) AS is_first
    FROM lastest_version_abertas
 )


, tmp_fatura_motivadora AS(
    SELECT 
        codigo_fatura_sydle AS fatura_motivadora
    FROM lastest_version_faturas_validas_mailing
    WHERE is_first = 1
) 

, tmp_faturas_em_aberto_e_divida_atual AS (
    SELECT
        codigo_contrato_air,
        concat_ws(', ', collect_list(codigo_fatura_sydle)) AS faturas_abertas,
         ROUND(SUM(valor_fatura),2) AS divida_atual
    FROM lastest_version_faturas_validas_mailing
    GROUP BY codigo_contrato_air
)

, last_pagamento AS (
    SELECT 
        codigo_contrato_air,
        CAST(MAX(data_pagamento) AS DATE) AS data_ultimo_pagamento
    FROM  {{ ref('tmp_faturas_all') }}
    WHERE data_atualizacao < '{{ dates.today }}'
    GROUP BY codigo_contrato_air
) 

, tmp_negociacao AS (
    SELECT 
        codigo_contrato_air,
        1 AS negociacao
    FROM  {{ ref('tmp_faturas_all') }}
    WHERE classificacao = 'Negociação'
    GROUP BY codigo_contrato_air
)

SELECT 
    fm.fatura_motivadora,
    lf.versao AS versao_fatura,
    lf.codigo_contrato_air,
    lf.data_vencimento AS data_vencimento_antigo,
    TRUNC(CAST(lf.data_vencimento AS DATE), 'MM') AS mes_vencimento,
    DATEDIFF(MONTH, TRUNC(CAST(lf.data_vencimento AS DATE), 'MM'), '{{ dates.today }}') AS aging_mes,
    lf.codigo_cliente_air,
    lf.status_fatura,
    lf.valor_fatura,
    CASE WHEN cli.tipo = 'FISICO' AND POSITION(' ' IN cli.nome) > 0
        THEN LEFT(cli.nome, POSITION(' ' IN cli.nome) - 1)
        ELSE cli.nome
    END AS nome_cliente,
    CASE WHEN cli.tipo = 'FISICO' 
        THEN 'CPF'
        ELSE 'CNPJ'
    END AS tipo_documento,
    COALESCE(cli.cpf, cli.cnpj) AS cpf_cnpj,
    COALESCE(neg.negociacao,0) AS negociacao,
    fada.faturas_abertas,
    fada.divida_atual,
    u.data_ultimo_pagamento
FROM tmp_fatura_motivadora as fm
INNER JOIN lastest_version_abertas lf
    ON fm.fatura_motivadora = lf.codigo_fatura_sydle
LEFT JOIN tmp_negociacao neg 
    ON lf.codigo_contrato_air = neg.codigo_contrato_air
LEFT JOIN tmp_faturas_em_aberto_e_divida_atual fada
    ON lf.codigo_contrato_air = fada.codigo_contrato_air
LEFT JOIN last_pagamento u
    ON lf.codigo_contrato_air = u.codigo_contrato_air
LEFT JOIN (
    SELECT
        id_cliente,
        tipo,
        nome,
        cpf,
        cnpj
    FROM {{ ref('dim_cliente') }}
)cli
    ON cli.id_cliente = lf.codigo_cliente_air
