select 
    datas_usadas.referencia,
    contrato_air,
    COUNT(contrato_air) qtd_pagamentos
FROM (
    SELECT 
        CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) AS inicio,
        DATE_SUB(CURRENT_DATE,1) AS referencia
) datas_usadas
LEFT JOIN gold.sydle.dim_faturas_mailing fm  
    ON fm.data_pagamento BETWEEN datas_usadas.inicio AND datas_usadas.referencia
GROUP BY ALL

UNION 

SELECT  
    *
FROM gold_dev.churn_voluntario.pagamentos
WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 