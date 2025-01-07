SELECT 
    datas_usadas.referencia,
    a.codigo_contrato as contrato_air,
    COUNT(a.codigo_contrato) AS qtd_visita_tecnica_60d
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
LEFT JOIN gold.chamados.dim_ordem a 
        ON CAST(a.data_abertura_os AS DATE) BETWEEN datas_usadas.inicio AND datas_usadas.referencia 
LEFT JOIN (
    select * 
    FROM (
        select 
            id_os,
            tipo,
            row_number() over (PARTITION by id_os order by id_os,momento_evento desc) as rankie 
        from bronze.air_chamado.tbl_os_evento
    )a where rankie = 1
) b ON a.codigo_os = b.id_os
WHERE 
    b.tipo LIKE '%FINALI%'
    AND a.id_tecnico IS NOT NULL
group by ALL

-- UNION 

-- SELECT  
--     *
-- FROM gold_dev.churn_voluntario.tbl_visita_tec
-- WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 