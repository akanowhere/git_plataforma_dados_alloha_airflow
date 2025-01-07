SELECT DISTINCT
    datas_usadas.referencia,
    codigo_contrato_air AS contrato_air,
    f.valor_fatura
FROM (
    SELECT 
    referencia
    FROM (SELECT explode(sequence(to_date('2024-05-31'), last_day(CURRENT_DATE), interval 1 month)) as referencia) meses
) datas_usadas 
LEFT JOIN (
    SELECT DISTINCT
        codigo_contrato_air, 
        valor_fatura,
        last_day(data_vencimento) data_vencimento,
        row_number() OVER( PARTITION BY codigo_contrato_air, last_day(data_vencimento) ORDER by last_day(data_vencimento) DESC) rn
    FROM gold.sydle.dim_faturas_all_versions
    WHERE codigo_contrato_air is not null
        AND data_vencimento IS NOT NULL
) f
ON data_vencimento = datas_usadas.referencia AND rn = 1

-- UNION 

-- SELECT  
--     *
-- FROM gold_dev.churn_voluntario.tbl_ult_pag
-- WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 