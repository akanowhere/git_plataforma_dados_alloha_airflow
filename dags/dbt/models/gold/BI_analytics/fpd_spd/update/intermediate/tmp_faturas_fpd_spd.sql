{{ config(
    materialized='ephemeral'
) }}

-- Montando Base Faturas
WITH DataCancelamentoFatura AS (
    SELECT 
        codigo_fatura_sydle,
        CAST(MIN(data_atualizacao) AS DATE) AS data_cancelamento_fatura
    FROM gold.sydle.dim_faturas_all_versions
    WHERE UPPER(status_fatura) = 'CANCELADA' and CAST(data_atualizacao as DATE) <= current_date()
    GROUP BY codigo_fatura_sydle
),
Faturas AS (
    SELECT 
        t1.codigo_contrato_air AS contrato_air,
        t1.codigo_fatura_sydle,
        t1.codigo_legado_fatura AS id_legado_fatura,
        CAST(REGEXP_REPLACE(t1.codigo_legado_fatura, '^(MOB_|VIP_NEGOCIACAO_|VIP_)', '') AS LONG) AS legado_fatura_trat,
        t1.classificacao,
        t1.status_fatura,
        t1.data_vencimento, 
        t1.data_pagamento,
        t1.valor_pago,
        CAST(t1.valor_sem_multa_juros AS DOUBLE) AS valor_fatura,
        CASE 
            WHEN UPPER(t1.status_fatura) IN ('INDEFINIDO', 'ABONADA', 'FATURA MÍNIMA', 'FATURA MINIMA') THEN 1
            WHEN UPPER(t1.status_fatura) = 'CANCELADA' AND t3.data_cancelamento_fatura <= t1.data_vencimento THEN 1		
            ELSE 0 
        END AS expurgo_sydle,
        t3.data_cancelamento_fatura,
        CAST(DATEDIFF(DAY, t1.data_vencimento, current_date()) AS int) AS aging,
        CAST(DATEDIFF(DAY, t1.data_vencimento, COALESCE(t1.data_pagamento, current_date())) AS int) AS aging_inadim,
        MIN(t1.data_pagamento) OVER (PARTITION BY t1.codigo_contrato_air, t1.codigo_legado_fatura) AS data_primeiro_pagamento    
    FROM gold.sydle.dim_faturas_all_versions t1    
    LEFT JOIN DataCancelamentoFatura t3 ON t1.codigo_fatura_sydle = t3.codigo_fatura_sydle
    WHERE t1.is_last = TRUE  
),
faturas_fpd_spd AS (
    SELECT  
        t1.contrato_air as codigo_contrato_air,
        t1.codigo_fatura_sydle,
        t1.codigo_fatura_sydle AS codigo_fatura,
        t1.data_vencimento,
        t1.data_pagamento,
        t1.data_primeiro_pagamento,
        t1.status_fatura,
        t1.valor_fatura,
        t1.data_cancelamento_fatura,
        DATE_FORMAT(data_vencimento,'yyyyMM') AS mes_vencimento,
        t1.aging_inadim,
        t1.aging,
        CASE
			WHEN aging >30 THEN '30d'
			WHEN aging >20 THEN '20d'
			WHEN aging >10 THEN '10d'
			WHEN aging >=1 THEN '1d'
			else '-'
		  END as flag_fb,
        CASE
			WHEN  aging_inadim >30 THEN '30d'
			WHEN  aging_inadim >20 THEN '20d'
			WHEN  aging_inadim >10 THEN '10d'
			WHEN  aging_inadim >=1 THEN '1d'
			else '-'
		  END flag_fpd,
        CASE
			WHEN  aging_inadim >30 THEN true
			else false 
		END flag_fpd30d,
        CASE
			WHEN  aging >30 THEN true
			else false
		END flag_fb30d,
        CASE
			WHEN  aging_inadim >20 THEN true
			else false 
		END flag_fpd20d,
        CASE
			WHEN  aging >20 THEN true
			else false 
		END flag_fb20d,
        CASE
			WHEN  aging_inadim >10 THEN true
			else false 
		END flag_fpd10d,
        CASE
			WHEN  aging >10 THEN true
			else false 
		END flag_fb10d,
        CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW())  AS data_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY t1.contrato_air ORDER BY t1.data_vencimento) AS parcela
    FROM Faturas t1
    WHERE expurgo_sydle = 0
)
SELECT *
FROM faturas_fpd_spd