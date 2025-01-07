select 
    referencia,
    contrato_air as contrato_air,
    ROUND(sum(do.valor_pago)/COUNT(do.valor_fatura), 2) as ticket_medio 
FROM (
    SELECT 
    referencia
    FROM (SELECT explode(sequence(to_date('2024-05-31'), last_day(CURRENT_DATE), interval 1 month)) as referencia) meses
) datas_usadas
LEFT JOIN  gold.sydle.dim_faturas_mailing do
    ON do.data_pagamento < datas_usadas.referencia
    AND do.contrato_air is not null
    AND do.data_pagamento IS NOT NULL
    and TRY_CAST(do.valor_fatura AS DECIMAL(5,2)) >= 30
group by all