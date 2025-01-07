WITH cte_tag AS (
	SELECT faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR,
	CASE
		--- FIRST BILL
		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL 
		AND status_contrato <> 'ST_CONT_CANCELADO'
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('INICIAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND MONTH(data_ativacao_contrato) = MONTH(DATEADD(DAY, -1, current_date()))
		AND YEAR(data_ativacao_contrato) = YEAR(DATEADD(DAY, -1, current_date()))
		THEN 'novas altas que geraram fatura - ativação em M0'

		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL 
		AND status_contrato <> 'ST_CONT_CANCELADO'
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('INICIAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND MONTH(data_ativacao_contrato) = MONTH(DATEADD(MONTH, -1, (DATEADD(DAY, -1, current_date()))))
		AND YEAR(data_ativacao_contrato) = YEAR(DATEADD(MONTH, -1, (DATEADD(DAY, -1, current_date()))))
		THEN 'novas altas que geraram fatura - ativação em M1'

		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL 
		AND status_contrato <> 'ST_CONT_CANCELADO'
		AND UPPER(classificacao_fatura) IN ('INICIAL', 'PEDIDO')
		AND data_ativacao_contrato < CAST(CONCAT((YEAR(DATEADD(MONTH, -1, (DATEADD(DAY, -1, current_date()))))), '-', (MONTH(DATEADD(MONTH, -1, (DATEADD(DAY, -1, current_date()))))), '-', '01') AS DATE)
		THEN 'novas altas que geraram fatura - ativação em outros meses'

		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL 
		AND status_contrato <> 'ST_CONT_CANCELADO'
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) not like '%PERMUTA%'
		AND porcentagem_desconto BETWEEN '1' AND '50'
		THEN 'colaboradores: desconto 1% a 50%'

		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL 
		AND status_contrato <> 'ST_CONT_CANCELADO'
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) not like '%PERMUTA%'
		AND porcentagem_desconto BETWEEN '51' AND '100'
		THEN 'colaboradores: desconto 51% a 100%'


		--- FATURADO AO DEFINIDO
		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL 
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND valor_fatura > COALESCE(faturamento_base_arvore_inicial.valor_final, faturamento_base_arvore_inicial.valor_base)
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		THEN 'faturado a maior ao definido'

		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND (valor_fatura - COALESCE(faturamento_base_arvore_inicial.valor_final, faturamento_base_arvore_inicial.valor_base)) BETWEEN -3 AND 3
		THEN 'faturado 0% de desconto'

		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND (faturamento_base_arvore_inicial.valor_fatura - valor_liquido_contrato) >= -3
		AND faturamento_base_arvore_inicial.valor_fatura < (COALESCE(faturamento_base_arvore_inicial.valor_final, faturamento_base_arvore_inicial.valor_base))
		AND ((faturamento_base_arvore_inicial.valor_fatura * 100) / (COALESCE(faturamento_base_arvore_inicial.valor_final, faturamento_base_arvore_inicial.valor_base))) BETWEEN '51' AND '99'
		THEN 'faturado 1% a 49% de desconto'

		WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND (faturamento_base_arvore_inicial.valor_fatura - valor_liquido_contrato) >= -3
		AND faturamento_base_arvore_inicial.valor_fatura < (COALESCE(faturamento_base_arvore_inicial.valor_final, faturamento_base_arvore_inicial.valor_base))
		AND ((faturamento_base_arvore_inicial.valor_fatura * 100) / (COALESCE(faturamento_base_arvore_inicial.valor_final, faturamento_base_arvore_inicial.valor_base))) BETWEEN '1' AND '50.9999'
		THEN 'faturado 50% a 100% de desconto'


		--- FATURADO <> AO DEFINIDO
		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND valor_fatura < valor_liquido_contrato
		AND FILHO.ID_CONTRATO_AIR_FILHO IS NOT NULL
		THEN 'contrato consolidado'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND valor_fatura < valor_liquido_contrato
		AND dias_suspensos > 0 and dias_suspensos < dias_base_calculo
		THEN 'suspensão parcial'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND valor_fatura < valor_liquido_contrato
		AND migracao_plano = 1
		THEN 'migração de outros planos'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND valor_fatura < valor_liquido_contrato
		AND ((UPPER(dim_faturas_itens.tipo_item) = 'AJUSTE COMERCIAL') OR (UPPER(dim_faturas_itens.tipo_item) = 'AJUSTE FINANCEIRO'))
		THEN 'faturados com valores de ajuste'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		THEN 'divergência'


		--NÃO FATURADOS
		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND CAST(DATEADD(HOUR, -3, current_date()) AS DATE) < dtCicloFim
		THEN 'não faturados até o momento'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND MONTH(data_ativacao_contrato) = MONTH(DATEADD(DAY, -1, current_date()))
		AND YEAR(data_ativacao_contrato) = YEAR(DATEADD(DAY, -1, current_date()))
		AND DATEDIFF(DAY, data_ativacao_contrato, dtCicloFim) < 9
		THEN 'altas pós ciclo'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND (UPPER(dim_contrato.marcador) LIKE '%PERMUTA%' OR UPPER(dim_contrato.marcador) LIKE '%CORTESIA%')
		THEN 'isenções - cortesias e permutas'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) like '%PERMUTA%'
		THEN 'isenções - cortesias e permutas'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) not like '%PERMUTA%'
		THEN 'isenções - colaborador alloha'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND porcentagem_desconto = '100'
		THEN 'isenções - desconto 100%'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND dias_suspensos > 0
		AND (dias_base_calculo - dias_suspensos) < 20
		THEN 'suspenso total'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
		AND (dias_base_calculo - dias_suspensos) > 10
		THEN 'outros não faturados'


		--CANCELADOS
		--FATURADOS NO MÊS
		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER(status_contrato) = 'ST_CONT_CANCELADO'
		AND UPPER(classificacao_fatura) = 'MANUAL'
		AND ((UPPER(dim_faturas_itens.nome_item) like '%REPOS%') OR (UPPER(dim_faturas_itens.tipo_item) = 'MULTA DE FIDELIDADE'))
		AND mes_referencia_fatura = 
		CASE
			WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
			THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
			ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
		END
		THEN 'cancelados - faturado no mês - multa de equipamento'


		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER(status_contrato) = 'ST_CONT_CANCELADO'
		AND UPPER(classificacao_fatura) IN ('FINAL', 'REFATURAMENTO')
		AND ((UPPER(dim_faturas_itens.nome_item) like '%REPOS%') OR (UPPER(dim_faturas_itens.tipo_item) = 'MULTA DE FIDELIDADE'))
		AND mes_referencia_fatura = 
		CASE
			WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
			THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
			ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
		END
		THEN 'cancelados - faturado no mês - multa de equipamento e last bill'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER(status_contrato) = 'ST_CONT_CANCELADO'
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('INICIAL', 'FINAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND mes_referencia_fatura = 
		CASE
			WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
			THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
			ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
		END
		THEN 'cancelados - faturado no mês - somente last bill'



		--FATURADOS OUTROS MESES
		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER(status_contrato) = 'ST_CONT_CANCELADO'
		AND UPPER(classificacao_fatura) = 'MANUAL'
		AND ((UPPER(dim_faturas_itens.nome_item) like '%REPOS%') OR (UPPER(dim_faturas_itens.tipo_item) = 'MULTA DE FIDELIDADE'))
		AND mes_referencia_fatura = 
		CASE
			WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))) = 6 
			THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date())))))
			ELSE CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))
		END
		THEN 'cancelados - faturado outros meses - multa de equipamento'


		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER(status_contrato) = 'ST_CONT_CANCELADO'
		AND UPPER(classificacao_fatura) IN ('FINAL', 'REFATURAMENTO')
		AND ((UPPER(dim_faturas_itens.nome_item) like '%REPOS%') OR (UPPER(dim_faturas_itens.tipo_item) = 'MULTA DE FIDELIDADE'))
		AND mes_referencia_fatura = 
		CASE
			WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))) = 6 
			THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date())))))
			ELSE CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))
		END
		THEN 'cancelados - faturado outros meses - multa de equipamento e last bill'

		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
		AND UPPER(status_contrato) = 'ST_CONT_CANCELADO'
		AND UPPER({{ translate_column('classificacao_fatura') }}) IN ('INICIAL', 'FINAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
		AND mes_referencia_fatura =
		CASE
			WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))) = 6 
			THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date())))))
			ELSE CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))
		END
		THEN 'cancelados - faturado outros meses - somente last bill'


		WHEN faturamento_base_arvore_inicial.codigo_fatura_sydle IS NULL
		AND UPPER(status_contrato) = 'ST_CONT_CANCELADO'
		THEN 'cancelados - não faturados'

		ELSE NULL END as tag

	FROM {{ ref('faturamento_base_arvore_inicial') }} as faturamento_base_arvore_inicial
	LEFT JOIN {{ ref('faturamento_air_contrato_campanha') }} as faturamento_air_contrato_campanha
		on faturamento_air_contrato_campanha.id_contrato = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
	LEFT JOIN {{ ref('vw_db_financeiro_tbl_aplicacao_cobranca_consolidada') }} AS FILHO
		ON faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR = FILHO.ID_CONTRATO_AIR_FILHO
	LEFT JOIN {{ ref('dim_contrato') }} as dim_contrato
		ON dim_contrato.id_contrato_air = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
	LEFT JOIN {{ ref('dim_faturas_itens') }} as dim_faturas_itens
		ON faturamento_base_arvore_inicial.codigo_fatura_sydle = dim_faturas_itens.codigo_fatura_sydle
),

tag_rn AS (
	SELECT CODIGO_CONTRATO_AIR,
	tag,
	ROW_NUMBER() OVER(PARTITION BY CODIGO_CONTRATO_AIR ORDER BY
						CASE
						WHEN tag = 'novas altas que geraram fatura - ativação em M0' then 1
						WHEN tag = 'novas altas que geraram fatura - ativação em M1' then 2
						WHEN tag = 'novas altas que geraram fatura - ativação em outros meses' then 3
						WHEN tag = 'colaboradores: desconto 1% a 50%' then 4
						WHEN tag = 'colaboradores: desconto 51% a 100%' then 5
						WHEN tag = 'faturado a maior ao definido' then 6
						WHEN tag = 'faturado 0% de desconto' then 7
						WHEN tag = 'faturado 1% a 49% de desconto' then 8
						WHEN tag = 'faturado 50% a 100% de desconto' then 9
						WHEN tag = 'contrato consolidado' then 10
						WHEN tag = 'suspensão parcial' then 11
						WHEN tag = 'migração de outros planos' then 12
						WHEN tag = 'faturados com valores de ajuste' then 13
						WHEN tag = 'divergência' then 14
						WHEN tag = 'não faturados até o momento' then 15
						WHEN tag = 'altas pós ciclo' then 16
						WHEN tag = 'isenções - cortesias e permutas' then 17
						WHEN tag = 'isenções - cortesias e permutas' then 18
						WHEN tag = 'isenções - colaborador alloha' then 19
						WHEN tag = 'isenções - desconto 100%' then 20
						WHEN tag = 'suspenso total' then 21
						WHEN tag = 'outros não faturados' then 22
						WHEN tag = 'cancelados - faturado no mês - multa de equipamento' then 23
						WHEN tag = 'cancelados - faturado no mês - multa de equipamento e last bill' then 24
						WHEN tag = 'cancelados - faturado no mês - somente last bill' then 25
						WHEN tag = 'cancelados - faturado outros meses - multa de equipamento' then 26
						WHEN tag = 'cancelados - faturado outros meses - multa de equipamento e last bill' then 27
						WHEN tag = 'cancelados - faturado outros meses - somente last bill' then 28
						WHEN tag = 'cancelados - não faturados' then 29
						ELSE 30 END) AS rn
	FROM cte_tag
)

SELECT CODIGO_CONTRATO_AIR,
	tag
FROM tag_rn
WHERE rn =1
