WITH vw_faturas_mailing AS (
    SELECT *
    FROM {{ ref('dim_faturas_all_versions') }} AS dim_faturas_all_versions
    WHERE is_last = TRUE
),

ult_pagamento AS (
    SELECT vw_faturas_mailing.codigo_contrato_air as contrato_air,
    DATE(vw_faturas_mailing.data_pagamento) AS data_pagamento,
    COUNT(codigo_fatura_sydle) AS qtd_ult_pagamento,
    SUM(valor_pago) AS valor_pago,
    SUM(valor_fatura) AS valor_fatura
    FROM vw_faturas_mailing
    INNER JOIN (
        SELECT codigo_contrato_air as contrato_air, MAX(CAST(data_pagamento AS DATE)) AS max_pagamento
        FROM vw_faturas_mailing
        WHERE UPPER(status_fatura) = 'PAGA' AND codigo_contrato_air IS NOT NULL AND codigo_contrato_air <> '0'
        GROUP BY codigo_contrato_air
    ) AS ult_pag
        ON vw_faturas_mailing.codigo_contrato_air = ult_pag.contrato_air
        AND vw_faturas_mailing.data_pagamento = max_pagamento
    GROUP BY vw_faturas_mailing.codigo_contrato_air , vw_faturas_mailing.data_pagamento
),

pri_pagamento AS (
    SELECT vw_faturas_mailing.codigo_contrato_air as contrato_air,
    DATE(vw_faturas_mailing.data_pagamento) AS data_pagamento,
    COUNT(codigo_fatura_sydle) AS qtd_pri_pagamento,
    SUM(valor_pago) AS valor_pago,
    SUM(valor_fatura) AS valor_fatura

    FROM vw_faturas_mailing
    INNER JOIN (
        SELECT codigo_contrato_air as contrato_air, MIN(CAST(data_pagamento AS DATE)) AS min_pagamento
        FROM vw_faturas_mailing
        WHERE UPPER(status_fatura) = 'PAGA' AND codigo_contrato_air IS NOT NULL AND codigo_contrato_air <> '0'
        GROUP BY codigo_contrato_air
    ) AS pri_pag
        ON vw_faturas_mailing.codigo_contrato_air = pri_pag.contrato_air
        AND vw_faturas_mailing.data_pagamento = min_pagamento
    GROUP BY vw_faturas_mailing.codigo_contrato_air, vw_faturas_mailing.data_pagamento
),

tot_pagamentos AS (
    SELECT codigo_contrato_air as contrato_air, COUNT(codigo_fatura_sydle) AS qtd_faturas_pagas, SUM(valor_pago) AS soma_valores_pagos
    FROM vw_faturas_mailing
    WHERE UPPER(status_fatura) = 'PAGA' AND codigo_contrato_air IS NOT NULL AND codigo_contrato_air <> '0'
    GROUP BY codigo_contrato_air
)

SELECT DISTINCT vw_faturas_mailing.codigo_contrato_air as contrato_air,
		ult_pagamento.data_pagamento AS dt_ult_pagamento,
		pri_pagamento.data_pagamento AS dt_pri_pagamento,
		ult_pagamento.qtd_ult_pagamento,
		ult_pagamento.valor_fatura AS valor_fatura_ult_pagamento,
		ult_pagamento.valor_pago AS valor_pago_ult_pagamento,
		pri_pagamento.qtd_pri_pagamento,
		pri_pagamento.valor_fatura AS valor_fatura_pri_pagamento,
		pri_pagamento.valor_pago AS valor_pago_pri_pagamento,
		DATEDIFF(DAY, DATE(ult_pagamento.data_pagamento), DATE(CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()))) AS tempo_desde_ult_pagamento,
		DATEDIFF(DAY, DATE(pri_pagamento.data_pagamento), DATE(CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()))) AS tempo_desde_pri_pagamento,
		tot_pagamentos.qtd_faturas_pagas AS qtd_tot_pagtos,
		tot_pagamentos.soma_valores_pagos AS vlr_tot_pagtos,
		CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS dt_atualizacao
FROM vw_faturas_mailing

--Informações da última fatura paga
LEFT JOIN ult_pagamento ON vw_faturas_mailing.codigo_contrato_air = ult_pagamento.contrato_air

--Informações da primeira fatura paga
LEFT JOIN pri_pagamento ON vw_faturas_mailing.codigo_contrato_air = pri_pagamento.contrato_air

--Quantidade de faturas pagas e valor somado pago, por contrato
LEFT JOIN tot_pagamentos ON vw_faturas_mailing.codigo_contrato_air = tot_pagamentos.contrato_air

WHERE vw_faturas_mailing.codigo_contrato_air IS NOT NULL AND vw_faturas_mailing.codigo_contrato_air <> '0'
