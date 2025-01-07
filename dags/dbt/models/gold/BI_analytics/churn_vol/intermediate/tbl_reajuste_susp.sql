SELECT 
    referencia,
    id_contrato AS contrato_air,
    COUNT(CASE WHEN tipo LIKE '%REAJUS%' THEN 1 END) qtd_reajuste_60d,
    COUNT(CASE WHEN tipo LIKE '%SUSP%' THEN 1 END) qtd_suspenso_60d
FROM (
    SELECT 
    DATE_ADD(meses.referencia, -60) inicio,
    referencia
    FROM (SELECT explode(sequence(to_date('2024-05-31'), last_day(CURRENT_DATE), interval 1 month)) as referencia) meses
) datas_usadas
-- FROM (
--     SELECT 
--         DATE_ADD(CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE), -60) inicio,
--         DATE_SUB(CURRENT_DATE,1) AS referencia
-- ) datas_usadas
LEFT JOIN gold.base.dim_cliente_evento 
ON momento BETWEEN datas_usadas.inicio AND datas_usadas.referencia
    AND tipo LIKE ANY ('%REAJUS%','%SUSP%')
GROUP BY referencia, id_contrato

-- UNION 

-- SELECT  *
-- FROM gold_dev.churn_voluntario.tbl_reajuste_susp
-- WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 