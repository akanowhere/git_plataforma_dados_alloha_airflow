SELECT 
    referencia,
    contrato_air,
    SUM(qtd_chamadas_total_st) AS qtd_chamadas_total_st,
    SUM(qtd_chamadas_total_sac) AS qtd_chamadas_total_sac
FROM (
    SELECT distinct 
        datas_usadas.referencia,
        CAST(REGEXP_REPLACE(contrato, '[^0-9]', '') AS decimal) AS contrato_air,
        COUNT(CASE WHEN UPPER(competencia) LIKE '%SUPORTE%' OR UPPER(competencia) LIKE '%ST%' THEN 1 END) qtd_chamadas_total_st,
        COUNT(CASE WHEN UPPER(competencia) LIKE '%SAC%' THEN 1 END) qtd_chamadas_total_sac 
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
    LEFT JOIN gold.five9.fato_chamadas_recebidas_voz fcrv
        ON CAST(fcrv.data_inicio_chamada AS DATE) BETWEEN datas_usadas.inicio AND datas_usadas.referencia
    WHERE 
        UPPER(fcrv.competencia) LIKE ANY('%SUPORTE%', '%ST%', '%SAC%')
        AND fcrv.contrato IS NOT NULL
    GROUP BY ALL
    UNION
    SELECT distinct 
        datas_usadas.referencia,
        CAST(REGEXP_REPLACE(contrato, '[^0-9]', '') AS decimal) as contrato_air,
        COUNT(CASE WHEN UPPER(competencia) LIKE '%SUPORTE%' OR UPPER(competencia) LIKE '%ST%' THEN 1 END) qtd_chamadas_total_st,
        COUNT(CASE WHEN UPPER(competencia) LIKE '%SAC%' THEN 1 END) qtd_chamadas_total_sac 
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
    LEFT JOIN gold.five9.fato_chamadas_chat fcrv
        ON CAST(fcrv.data_inicio_chamada AS DATE) BETWEEN datas_usadas.inicio AND datas_usadas.referencia
    WHERE 
        UPPER(fcrv.competencia) LIKE ANY('%SUPORTE%', '%ST%', '%SAC%')
        AND fcrv.contrato IS NOT NULL
    GROUP BY ALL 
)
GROUP BY ALL

-- UNION 

-- SELECT  
--     *
-- FROM gold_dev.churn_voluntario.tbl_sac_suporte
-- WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 