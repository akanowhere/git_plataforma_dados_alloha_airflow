SELECT 
    referencia,
    id_contrato AS contrato_air,
    COUNT(1) qtd_solici_susp
FROM (
    SELECT 
        CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) AS inicio,
        DATE_SUB(CURRENT_DATE,1) AS referencia
) datas_usadas
LEFT JOIN gold.base.dim_cliente_evento 
ON momento BETWEEN datas_usadas.inicio AND datas_usadas.referencia
    AND tipo = 'EVT_CONTRATO_SUSPENSO' and observacao like 'Solicitação de suspensão%'
GROUP BY ALL

UNION 

SELECT  
    *
FROM gold_dev.churn_voluntario.solicitacao_susp
WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 