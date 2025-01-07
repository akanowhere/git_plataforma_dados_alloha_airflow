SELECT DISTINCT
    datas_usadas.referencia,
    codigo_contrato_air AS contrato_air,
    FIRST(data_pagamento) OVER (PARTITION BY codigo_contrato_air ORDER BY data_pagamento DESC) AS data_ultimo_pagamento
FROM (
    SELECT 
    referencia
    FROM (SELECT explode(sequence(to_date('2024-05-31'), last_day(CURRENT_DATE), interval 1 month)) as referencia) meses
) datas_usadas 
LEFT JOIN (
    SELECT DISTINCT
        codigo_contrato_air, 
        data_pagamento
    FROM gold.sydle.dim_faturas_all_versions
    WHERE codigo_contrato_air is not null
        AND data_pagamento IS NOT NULL
) f
ON data_pagamento <= datas_usadas.referencia 

-- UNION 

-- SELECT  
--     *
-- FROM gold_dev.churn_voluntario.tbl_ult_pag
-- WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 