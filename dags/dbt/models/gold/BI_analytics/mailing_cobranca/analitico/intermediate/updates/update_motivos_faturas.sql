{{ 
    config(
        materialized='ephemeral'
    ) 
}}

{% set dates = get_dates(['first_day_month', 'today'], None, 'updates_faturas') %}

WITH lastest_version_faturas AS (
    SELECT 
        *
    FROM  {{ ref('tmp_faturas_all') }}
)
, update_baixa_operacional AS (		
	SELECT 
		fatura_motivadora,
		'{{ dates.today }}' AS data_saida,
		CAST(fm.data_negociacao AS DATE) AS data_motivo_saida,
		CAST(fm.data_atualizacao AS DATE) AS data_atualizacao_motivo_saida,
		'BAIXA_OPERACIONAL' AS motivo_saida,
		'FATURA' AS saida_tipo,
		mi.data_pagamento,
		mi.valor_pago,
		mi.tipo_pagamento
	FROM {{ ref('tmp_mailing_update') }} mi
	LEFT JOIN lastest_version_faturas fm
		ON mi.id_contrato = fm.codigo_contrato_air 
		AND mi.fatura_motivadora = fm.codigo_fatura_sydle 
		--AND mi.versao_fatura < fm.versao
	WHERE  mi.data_saida IS NULL
		AND fm.status_fatura = 'BAIXA OPERACIONAL'
		AND CAST(fm.data_atualizacao AS DATE) < '{{ dates.today }}'
)
, update_pagamento_espontaneo AS (
	SELECT  
		fatura_motivadora,
		CASE 
			WHEN mi.data_saida IS NULL
				THEN '{{ dates.today }}'
			ELSE mi.data_saida 
		END AS data_saida,
		CASE 
			WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
				THEN CAST(fm.data_pagamento AS DATE)
			ELSE mi.data_motivo_saida 
		END AS data_motivo_saida,
		CASE 
			WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
				THEN CAST(fm.data_atualizacao AS DATE)
			ELSE CAST(mi.data_atualizacao_motivo_saida AS DATE) 
		END data_atualizacao_motivo_saida,
		CASE 
			WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
				THEN 'PAGAMENTO'
			ELSE mi.motivo_saida 
		END motivo_saida,
		CASE 
			WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
				THEN 'FATURA'
			ELSE mi.saida_tipo 
		END saida_tipo,
		CAST(fm.data_pagamento AS DATE) AS data_pagamento,
		fm.valor_pago,
		'ESPONTÂNEO' AS tipo_pagamento
	FROM {{ ref('tmp_mailing_update') }} mi
	LEFT JOIN lastest_version_faturas fm 
		ON mi.id_contrato = fm.codigo_contrato_air 
		AND mi.fatura_motivadora = fm.codigo_fatura_sydle 
		--AND mi.versao_fatura < fm.versao
	WHERE mi.data_saida IS NULL
		AND fm.status_fatura = 'PAGA'
		AND CAST(fm.data_atualizacao AS DATE) < '{{ dates.today }}'
)
, tmp_negociacoes AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY fatura_original ORDER BY data_negociacao DESC) rn 
    FROM {{ get_catalogo('gold') }}.cobranca.cobranca_negociacoes
)
, update_negociacao AS (	
	SELECT  
		fatura_motivadora,
		'{{ dates.today }}' AS data_saida,
		CAST(cn.data_negociacao AS DATE) AS data_motivo_saida,
		CAST(cn.dt_atualizacao AS DATE) AS data_atualizacao_motivo_saida,
		'NEGOCIAÇÃO' AS motivo_saida,
		'FATURA' AS saida_tipo,
		mi.data_pagamento,
		mi.valor_pago,
		mi.tipo_pagamento
	FROM {{ ref('tmp_mailing_update') }} mi
	LEFT JOIN tmp_negociacoes cn
		ON cn.fatura_original = fm.codigo_fatura_sydle
		AND mi.id_contrato = cn.contrato_air 
		AND cn.rn = 1
	WHERE  
		mi.data_saida IS NULL
		AND CAST(cn.dt_atualizacao AS DATE) < '{{ dates.today }}'
		AND DATE_FORMAT(cn.data_negociacao, 'yyyy-MM') = DATE_FORMAT(DATE_SUB('{{ dates.today }}',1), 'yyyy-MM')
)
, update_abono AS (
	SELECT  
		fatura_motivadora,
		'{{ dates.today }}' AS data_saida,
		CAST(fm.data_negociacao AS DATE) AS data_motivo_saida,
		CAST(fm.data_atualizacao AS DATE) AS data_atualizacao_motivo_saida,
		'ABONO' AS motivo_saida,
		'FATURA' AS saida_tipo,
		mi.data_pagamento,
		mi.valor_pago,
		mi.tipo_pagamento
	FROM {{ ref('tmp_mailing_update') }} mi
	LEFT JOIN lastest_version_faturas fm
		ON mi.id_contrato = fm.codigo_contrato_air 
		AND mi.fatura_motivadora = fm.codigo_fatura_sydle
		--AND mi.versao_fatura < fm.versao
	WHERE  mi.data_saida IS NULL
		AND fm.status_fatura = 'ABONADA'
		AND CAST(fm.data_atualizacao AS DATE) < '{{ dates.today }}'
)
, update_cancelamento AS(
	SELECT  
		fatura_motivadora,
		'{{ dates.today }}' AS data_saida,
		CAST(fm.data_negociacao AS DATE) AS data_motivo_saida,
		CAST(fm.data_atualizacao AS DATE) AS data_atualizacao_motivo_saida,
		'CANCELAMENTO' AS motivo_saida,
		'FATURA' AS saida_tipo,
		mi.data_pagamento,
		mi.valor_pago,
		mi.tipo_pagamento
	FROM {{ ref('tmp_mailing_update') }} mi
	LEFT JOIN lastest_version_faturas fm
		ON mi.id_contrato = fm.codigo_contrato_air 
		AND mi.fatura_motivadora = fm.codigo_fatura_sydle 
		--AND mi.versao_fatura < fm.versao
	WHERE  mi.data_saida IS NULL
		AND fm.status_fatura = 'CANCELADA'
		AND CAST(fm.data_atualizacao AS DATE) < '{{ dates.today }}'
)

, update_pagamento_negociacoes AS (
	SELECT 
		mi.fatura_motivadora,
		mi.data_saida,
		mi.data_motivo_saida,
		mi.data_atualizacao_motivo_saida,
		mi.motivo_saida,
		mi.saida_tipo,
		CAST(fm.data_pagamento AS DATE) AS data_pagamento,
		fm.valor_pago,
		'NEGOCIAÇÃO' AS tipo_pagamento
	FROM {{ ref('tmp_mailing_update') }} mi
	LEFT JOIN (
		SELECT DISTINCT
				m.fatura_motivadora,
				neg.codigo
		FROM {{ ref('tmp_mailing_update') }} m
		LEFT JOIN gold_dev.sydle.dim_negociacoes neg
			ON m.fatura_motivadora = neg.fatura_original
		WHERE m.motivo_saida = 'NEGOCIAÇÃO'
		AND m.data_referencia ='{{ dates.first_day_month}}'
	) as a
		ON mi.fatura_motivadora = a.fatura_motivadora
	LEFT JOIN lastest_version_faturas fm
		ON fm.codigo_fatura_sydle = a.fatura_motivadora
	WHERE 
		a.fatura_motivadora IS NOT NULL
		AND fm.data_pagamento IS NOT NULL
)
, final_table AS (
    SELECT * FROM update_baixa_operacional
    UNION 
    SELECT * FROM update_pagamento_espontaneo
    UNION 
    SELECT * FROM update_negociacao
    UNION 
    SELECT * FROM update_abono
    UNION 
    SELECT * FROM update_cancelamento
    UNION 
    SELECT * FROM update_pagamento_negociacoes
)
SELECT 
	fatura_motivadora,
    data_saida,
    data_motivo_saida,
	CAST(data_atualizacao_motivo_saida AS DATE) AS data_atualizacao_motivo_saida,
    saida_tipo,
    motivo_saida,
    data_pagamento,
    tipo_pagamento,
	valor_pago,
    CAST(NULL AS DATE) AS data_cancelamento_contrato,
    CAST('' AS STRING) AS motivo_cancelamento_contrato
FROM final_table


