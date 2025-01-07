SELECT 
    datas_usadas.referencia,
    dc.codigo_contrato AS contrato_air,
    COUNT(dc.codigo_contrato) AS qtd_chamados_financeiros_60d,
    COUNT(CASE WHEN dc.data_conclusao is null THEN 1 END) AS qtd_chamados_financeiros_abertos_60d
FROM (
    SELECT 
        CAST(date_add(MONTH, -2, meses.referencia) AS DATE) as inicio,
        referencia
    FROM (
        SELECT explode(sequence(to_date('2024-05-31'), last_day(CURRENT_DATE), interval 1 month)) as referencia
    ) meses
) datas_usadas
-- FROM (
--     SELECT 
--         DATE_ADD(CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE), -60) inicio,
--         DATE_SUB(CURRENT_DATE,1) AS referencia
-- ) datas_usadas
LEFT JOIN gold.chamados.dim_chamado dc 
        ON CAST(dc.data_abertura AS DATE) BETWEEN datas_usadas.inicio AND datas_usadas.referencia 
WHERE  
    nome_fila IN (
        'ANÁLISE DE COMPROVANTE - CALL CENTER',
        'FINANCEIRO - ANÁLISE COMPROVANTE PGTO',
        'FINANCEIRO - COBRANÇA DE EQUIPAMENTOS/SERVIÇOS',
        'FINANCEIRO - CONTESTAÇÃO DE FATURA',
        'FINANCEIRO - CORREÇÃO DE ENDEREÇO NA FATURA',
        'FINANCEIRO - CORTESIAS',
        'FINANCEIRO - DÉBITO AUTOMÁTICO',
        'FINANCEIRO - DESCONTO PROMOCIONAL NÃO INTEGRADO',
        'FINANCEIRO - DESCONTO RETENÇÃO',
        'FINANCEIRO - DIAS SEM CONEXÃO DESCONTO (PROBLEMA TÉCNICO)',
        'FINANCEIRO - DOCUMENTO QUITAÇÃO DE DÉBITO',
        'FINANCEIRO - ENVIO NF B2C/PME',
        'FINANCEIRO - FATURA MÍNIMA',
        'FINANCEIRO - FATURA NÃO DISPONÍVEL',
        'FINANCEIRO - FATURA NÃO GERADA',
        'FINANCEIRO - INDIQUE UM AMIGO',
        'FINANCEIRO - PAGAMENTO EM DUPLICIDADE',
        'FINANCEIRO - PAGAMENTO NÃO BAIXADO',
        'FINANCEIRO - PAGAMENTO TROCADO',
        'FINANCEIRO - PLANO NÃO INTEGRADO',
        'FINANCEIRO - TELEFONIA (OST)',
        'FINANCEIRO - VALOR PROPORCIONAL -TROCA DE PLANO',
        'FINANCEIRO -OUVIDORIA/JURIDICO',
        'FINANCEIRO REEMBOLSO',
        'FINANCEIRO - REMOÇÃO FATURA DE EQUIPAMENTO',
        'BKO - PENDÊNCIA',
        'FINANCEIRO - ANÁLISE DE FRAUDE'
    )
    AND CAST(data_abertura AS DATE) >= DATE_ADD(to_date('2024-05-31'), -60)
GROUP BY ALL

-- UNION 

-- SELECT  
--     *
-- FROM gold_dev.churn_voluntario.tbl_chamado_financeiro
-- WHERE referencia < CAST(DATE_TRUNC('month', DATE_SUB(CURRENT_DATE,1)) AS DATE) 