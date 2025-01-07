WITH temp_fatura AS (
    SELECT 
        codigo_fatura_sydle,
        contrato_air,
        valor_sem_multa_juros,
        status_fatura,
        data_vencimento,
        classificacao
    FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_mailing
    WHERE CAST(data_atualizacao AS DATE) >= '2023-01-01'
)
SELECT 
    t1.id_negociacao AS codigo_negociacao,
    CAST(t2.contrato_air AS BIGINT) AS codigo_contrato_air,
    CAST(t1.codigo AS BIGINT) AS codigo_fatura,
    CAST(t2.valor_sem_multa_juros AS DOUBLE) AS valor_fatura,
    t2.status_fatura AS status_fatura,
    CAST(t2.data_vencimento AS DATE) AS data_vencimento_fatura,
    CAST(t1.fatura_original AS BIGINT) AS codigo_fatura_original,
    CAST(t3.valor_sem_multa_juros AS DOUBLE) AS valor_fatura_original,
    CAST(t3.data_vencimento AS DATE) AS data_vencimento_fatura_original,
    t3.classificacao AS classificacao_fatura_original,
    t1.id_negociador AS codigo_negociador,
    t1.nm_negociador AS nome_negociador,
    t1.data_negociacao,
    date_sub(current_date(), 1) AS data_referencia
FROM {{ get_catalogo('gold') }}.sydle.dim_negociacao t1
INNER JOIN temp_fatura t2
    ON t1.codigo = t2.codigo_fatura_sydle
INNER JOIN temp_fatura t3
    ON t1.fatura_original = t3.codigo_fatura_sydle
WHERE t1.data_negociacao >= '2023-01-01'
