{{
    config(
        materialized='ephemeral'
    )
}}

{% set dates = get_dates(['first_day_month', 'today'],None, 'mailing_inicial') %}

WITH tmp_mailing_INICIAL(
    SELECT
		cem.status_contrato,
		cem.id_contrato,
		cem.id_cliente,
		cem.status_mailing,
		cem.versao_contrato,
		cem.marca,
		cem.cidade,
		cem.regional,
		cem.localidade,
        fem.fatura_motivadora,
        fem.versao_fatura,
        fem.data_vencimento_antigo,
        fem.mes_vencimento,
        fem.aging_mes,
        fem.status_fatura,
        fem.valor_fatura,
        fem.nome_cliente,
        fem.tipo_documento,
        fem.cpf_cnpj,
		fem.negociacao,
        fem.faturas_abertas,
        fem.divida_atual,
        fem.data_ultimo_pagamento,
        CASE 
            WHEN cem.status_mailing = 'ATIVO' AND  fem.aging_mes >=3
                THEN '61-90d+'	
            WHEN cem.status_mailing = 'INATIVO' AND  fem.aging_mes > 24
                THEN '720d+'	
            ELSE
                    CONCAT((fem.aging_mes*30 - 29),'-',(fem.aging_mes*30),'d')	
        END faixa_aging,
		CAST( '{{dates.first_day_month}}' AS DATE) data_referencia
    FROM {{ ref('tmp_contratos_elegiveis_mailing') }} cem
    INNER JOIN {{ ref('tmp_faturas_elegiveis_mailing') }} fem
        ON (cem.id_contrato = fem.codigo_contrato_air AND cem.id_cliente = fem.codigo_cliente_air)
)

--### Finalização Mailing ###--
, final_table AS (
	SELECT 
		DISTINCT
		marca,
		mi.id_contrato,
		versao_contrato,
		status_contrato,
		nome_cliente,
		id_cliente,
		cpf_cnpj,
		tipo_documento,
		fatura_motivadora,
		versao_fatura,
		status_fatura,
		mes_vencimento,
		data_vencimento_antigo,
		data_ultimo_pagamento,
		negociacao,
		aging_mes,
		divida_atual,
		faturas_abertas,
		cidade,
		regional,
		CASE WHEN uce.ultima_chance = 1 THEN CONCAT(mi.faixa_aging,'[ULTIMA_CHANCE]') ELSE mi.faixa_aging END faixa_aging,
		status_mailing,
		localidade,
		COALESCE(uce.ultima_chance, 0) ultima_chance,
		COALESCE(uce.faixa_mailing_anterior, '') faixa_mailing_anterior,
		0 fila_ouvidoria_contestacao_fatura,
		CAST(NULL AS DATE) data_saida,
		CAST(NULL AS DATE) data_motivo_saida,
		CAST(NULL AS DATE) data_atualizacao_motivo_saida,
		CAST(NULL AS DATE) data_cancelamento_contrato,
		CAST(NULL AS DATE) data_pagamento,
		CAST('' AS STRING) AS saida_tipo,
		CAST('' AS STRING) AS motivo_saida,
		CAST('' AS STRING) AS tipo_pagamento,
		CAST('' AS STRING) AS motivo_cancelamento_contrato,
		valor_fatura valor_fatura_motivadora,
		data_referencia,
		CAST(0.0 AS DOUBLE)AS  valor_pago
	FROM tmp_mailing_INICIAL mi
	LEFT JOIN {{ ref('tmp_uce') }} uce
		ON mi.id_contrato = uce.id_contrato
)
SELECT 
	*
FROM final_table