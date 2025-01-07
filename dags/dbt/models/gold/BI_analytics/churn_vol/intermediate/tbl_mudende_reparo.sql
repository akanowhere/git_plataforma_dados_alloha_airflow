SELECT  
    datas_usadas.referencia,
    dc.codigo_contrato as contrato_air,
    COUNT(CASE WHEN dc.servico LIKE '%END%' THEN 1 END) AS qtd_mudend_60d,
    COUNT(CASE WHEN dc.fila LIKE '%REPARO%' THEN 1 END) AS qtd_reparo_60d
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
LEFT JOIN gold.chamados.dim_ordem dc 
    ON CAST(dc.data_criacao_os AS DATE) BETWEEN datas_usadas.inicio AND datas_usadas.referencia
WHERE
    dc.servico LIKE '%ENDER%'
    OR dc.fila LIKE '%REPARO%'
group by ALL

-- UNION 

-- SELECT  
--     *
-- FROM gold_dev.churn_voluntario.tbl_mudende_reparo
-- WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 