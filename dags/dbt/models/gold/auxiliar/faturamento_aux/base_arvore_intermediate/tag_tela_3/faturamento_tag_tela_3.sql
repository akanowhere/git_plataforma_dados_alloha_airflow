WITH classificacao_M1 AS (
	SELECT B.CODIGO_CONTRATO_AIR, B.mes_referencia,
	'M1' AS classificacao
	FROM {{ get_catalogo('gold') }}.faturamento.base_faturas_meses_passados B
	where DATEDIFF(MONTH,
		CAST((CONCAT(YEAR(B.emissao_fatura), '-', MONTH(B.emissao_fatura), '-01'))AS DATE),
		CAST((CONCAT(RIGHT(B.mes_referencia, 4), '-', LEFT(B.mes_referencia, 2), '-01'))AS DATE)) = 1
),

classificacao_M2 AS (
	SELECT B.CODIGO_CONTRATO_AIR, B.mes_referencia,
	'M2' AS classificacao
	FROM {{ get_catalogo('gold') }}.faturamento.base_faturas_meses_passados B
	where DATEDIFF(MONTH,
		CAST((CONCAT(YEAR(B.emissao_fatura), '-', MONTH(B.emissao_fatura), '-01'))AS DATE),
		CAST((CONCAT(RIGHT(B.mes_referencia, 4), '-', LEFT(B.mes_referencia, 2), '-01'))AS DATE)) = 2
),

classificacao_M3 AS (
	SELECT B.CODIGO_CONTRATO_AIR, B.mes_referencia,
	'M3' AS classificacao
	FROM {{ get_catalogo('gold') }}.faturamento.base_faturas_meses_passados B
	where DATEDIFF(MONTH,
		CAST((CONCAT(YEAR(B.emissao_fatura), '-', MONTH(B.emissao_fatura), '-01'))AS DATE),
		CAST((CONCAT(RIGHT(B.mes_referencia, 4), '-', LEFT(B.mes_referencia, 2), '-01'))AS DATE)) = 3
),

classificacao_FATURAS_OUTROS_MESES AS (
	SELECT B.CODIGO_CONTRATO_AIR, B.mes_referencia,
	'FATURADOS_OUTROS_MESES' AS classificacao
	FROM {{ get_catalogo('gold') }}.faturamento.base_faturas_meses_passados B
	where DATEDIFF(MONTH,
		CAST((CONCAT(YEAR(B.emissao_fatura), '-', MONTH(B.emissao_fatura), '-01'))AS DATE),
		CAST((CONCAT(RIGHT(B.mes_referencia, 4), '-', LEFT(B.mes_referencia, 2), '-01'))AS DATE)) > 3
),

classificacao_SEM_FATURAS AS (
	SELECT A.CODIGO_CONTRATO_AIR, A.mes_referencia_emissao,
	'SEM_FATURAS' AS classificacao
	FROM {{ get_catalogo('gold') }}.auxiliar.faturamento_base_arvore_inicial A
	LEFT JOIN {{ get_catalogo('gold') }}.faturamento.base_faturas_meses_passados B
		ON A.CODIGO_CONTRATO_AIR = B.CODIGO_CONTRATO_AIR AND A.mes_referencia_emissao = B.mes_referencia
	LEFT JOIN {{ get_catalogo('gold') }}.auxiliar.faturamento_tag AS tag ON A.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
	where UPPER(tag.tag) = 'SUSPENSO TOTAL' AND B.codigo_fatura_sydle IS NULL
),

JUNCAO_CLASSIFICACAO AS (
	SELECT classificacao_M1.*
	FROM classificacao_M1
	UNION
	SELECT classificacao_M2.*
	FROM classificacao_M2
	UNION
	SELECT classificacao_M3.*
	FROM classificacao_M3
	UNION
	SELECT classificacao_FATURAS_OUTROS_MESES.*
	FROM classificacao_FATURAS_OUTROS_MESES
	UNION
	SELECT classificacao_SEM_FATURAS.*
	FROM classificacao_SEM_FATURAS
),

CLASSIFICACAO_FINAL AS (
SELECT CODIGO_CONTRATO_AIR, mes_referencia,
CASE
	WHEN CODIGO_CONTRATO_AIR IN (SELECT jc.CODIGO_CONTRATO_AIR
								FROM JUNCAO_CLASSIFICACAO jc
								WHERE jc.classificacao = 'M1'
								AND jc.CODIGO_CONTRATO_AIR = JUNCAO_CLASSIFICACAO.CODIGO_CONTRATO_AIR
								AND jc.mes_referencia = JUNCAO_CLASSIFICACAO.mes_referencia)
	THEN 'M1'
	WHEN CODIGO_CONTRATO_AIR IN (SELECT jc.CODIGO_CONTRATO_AIR
								FROM JUNCAO_CLASSIFICACAO jc
								WHERE jc.classificacao = 'M2'
								AND jc.CODIGO_CONTRATO_AIR = JUNCAO_CLASSIFICACAO.CODIGO_CONTRATO_AIR
								AND jc.mes_referencia = JUNCAO_CLASSIFICACAO.mes_referencia)
	THEN 'M2'
	WHEN CODIGO_CONTRATO_AIR IN (SELECT jc.CODIGO_CONTRATO_AIR
								FROM JUNCAO_CLASSIFICACAO jc
								WHERE jc.classificacao = 'M3'
								AND jc.CODIGO_CONTRATO_AIR = JUNCAO_CLASSIFICACAO.CODIGO_CONTRATO_AIR
								AND jc.mes_referencia = JUNCAO_CLASSIFICACAO.mes_referencia)
	THEN 'M3'
	WHEN CODIGO_CONTRATO_AIR IN (SELECT jc.CODIGO_CONTRATO_AIR
								FROM JUNCAO_CLASSIFICACAO jc
								WHERE jc.classificacao = 'FATURADOS_OUTROS_MESES'
								AND jc.CODIGO_CONTRATO_AIR = JUNCAO_CLASSIFICACAO.CODIGO_CONTRATO_AIR
								AND jc.mes_referencia = JUNCAO_CLASSIFICACAO.mes_referencia)
	THEN 'FATURADOS_OUTROS_MESES'
	WHEN CODIGO_CONTRATO_AIR IN (SELECT jc.CODIGO_CONTRATO_AIR
								FROM JUNCAO_CLASSIFICACAO jc
								WHERE jc.classificacao = 'SEM_FATURAS'
								AND jc.CODIGO_CONTRATO_AIR = JUNCAO_CLASSIFICACAO.CODIGO_CONTRATO_AIR
								AND jc.mes_referencia = JUNCAO_CLASSIFICACAO.mes_referencia)
	THEN 'SEM_FATURAS'
	ELSE NULL end classificacao

FROM JUNCAO_CLASSIFICACAO
GROUP BY CODIGO_CONTRATO_AIR, mes_referencia
),

suspensao_total AS (
	SELECT faturamento_base_arvore_inicial.*
	FROM {{ get_catalogo('gold') }}.auxiliar.faturamento_base_arvore_inicial
	LEFT JOIN {{ get_catalogo('gold') }}.auxiliar.faturamento_tag as tag ON faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
	where UPPER(tag.tag) = 'SUSPENSO TOTAL'
),

classificacao_suspensos AS (
	SELECT
	A.CODIGO_CONTRATO_AIR,
	A.mes_referencia_emissao,
	B.classificacao
	FROM suspensao_total A
	LEFT JOIN CLASSIFICACAO_FINAL B
	ON A.CODIGO_CONTRATO_AIR = B.CODIGO_CONTRATO_AIR
		AND A.mes_referencia_emissao = B.mes_referencia
),

M1 AS (
	SELECT CODIGO_CONTRATO_AIR
	FROM {{ get_catalogo('gold') }}.faturamento.base_arvore_faturas
	WHERE codigo_fatura_sydle IS NOT NULL
	AND mes_referencia_emissao =
	CASE
		WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))) = 6 
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date())))))
		ELSE CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))
	END
	GROUP BY CODIGO_CONTRATO_AIR
),

M1_PAGOS AS (
	SELECT CODIGO_CONTRATO_AIR, COUNT(codigo_fatura_sydle) AS qtd_faturas
	FROM {{ get_catalogo('gold') }}.faturamento.base_faturas
	WHERE codigo_fatura_sydle IS NOT NULL AND UPPER(status_fatura) = 'PAGA'
	AND mes_referencia_emissao =
	CASE
		WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))) = 6 
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date())))))
		ELSE CONCAT(MONTH(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, +1, DATEADD(DAY, -1, current_date()))))
	END
	GROUP BY CODIGO_CONTRATO_AIR
),

cte_tag_tela_3 AS (
	SELECT faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR,
	CASE
			WHEN tag.tag = 'suspenso total'
			AND classificacao_suspensos.classificacao = 'M1'
			THEN 'Suspensão Integral M1'

			WHEN tag.tag = 'suspenso total'
			AND classificacao_suspensos.classificacao = 'M2'
			THEN 'Suspensão Integral M2'

			WHEN tag.tag = 'suspenso total'
			AND classificacao_suspensos.classificacao not in ( 'M1', 'M2')
			THEN 'Suspensão Integral M3+'

			WHEN tag.tag = 'suspensão parcial'
			AND M1.CODIGO_CONTRATO_AIR IS NULL
			THEN 'Suspensão Parcial - Retorno'

			WHEN  tag.tag = 'suspensão parcial'
			THEN 'Suspensão Parcial'

			WHEN tag.tag LIKE 'novas altas que geraram fatur%'
			THEN 'Nova Ativação'

			WHEN tag.tag = 'altas pós ciclo'
			THEN 'Entrante Pós Ciclo'

			WHEN tag.tag LIKE '%multa de equipamento e last bill%'
			AND faturamento_base_arvore_inicial.tipo_multa = 'Equipamento'
			THEN 'Multas e Last Bill'

			WHEN tag.tag LIKE '%multa de equipamento%'
			AND faturamento_base_arvore_inicial.tipo_multa = 'Equipamento'
			THEN 'Multas - Equipamento'

			WHEN tag.tag LIKE '%multa de equipamento%'
			AND faturamento_base_arvore_inicial.tipo_multa = 'Fidelidade'
			THEN 'Multas - Fidelidade'

			WHEN tag.tag LIKE '%cancelados - faturado outros meses%'
			THEN 'Last Bill M1'

			WHEN tag.tag LIKE '%cancelados - faturado no mês%'
			THEN 'Last Bill M0'

			WHEN tag.tag = 'isenções - desconto 100%'
			THEN 'Desconto 100%'

			WHEN UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND UPPER(base_faturas_migracao.natureza_venda) = 'VN_MIGRACAO_DOWN'
			THEN 'Downgrade'

			WHEN UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND UPPER(base_faturas_migracao.natureza_venda) = 'VN_MIGRACAO_UP'
			THEN 'Upgrade'

			WHEN UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND UPPER(base_faturas_migracao.natureza_venda) = 'VN_MIGRACAO_COMPULSORIA'
			THEN 'Migração Compulsória'

			WHEN UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND base_faturas_reajustes.CODIGO_CONTRATO_AIR IS NOT NULL
			THEN 'Reajuste'

			WHEN UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND base_faturas_venda_adicional.CODIGO_CONTRATO_AIR IS NOT NULL
			THEN 'Adição de Produto'

			WHEN UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND base_faturas_alteracao_vencimento.CODIGO_CONTRATO_AIR IS NOT NULL
			THEN 'Alteração de Vencimento'

			WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
			AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND YEAR(base_faturas_desconto.dia_aplicar) = YEAR(DATEADD(DAY, -1, current_date()))
			AND MONTH(base_faturas_desconto.dia_aplicar) = MONTH(DATEADD(DAY, -1, current_date()))
			THEN 'Aplicação de Desconto'

			WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
			AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND YEAR(base_faturas_desconto.data_validade) = YEAR(DATEADD(DAY, -1, current_date()))
			AND MONTH(base_faturas_desconto.data_validade) = MONTH(DATEADD(DAY, -1, current_date()))
			THEN 'Fim do Desconto'

			WHEN  faturamento_base_arvore_inicial.codigo_fatura_sydle IS NOT NULL
			AND UPPER(status_contrato) <> 'ST_CONT_CANCELADO'
			AND M1_PAGOS.qtd_faturas > 1
			AND faturamento_base_arvore_inicial.valor_fatura < faturamento_base_arvore_inicial.valor_liquido_contrato
			THEN 'Duplicado'

			ELSE 'sem classificação' END AS tag_tela_3

	FROM {{ ref('faturamento_base_arvore_inicial') }} as faturamento_base_arvore_inicial

	LEFT JOIN M1 ON M1.CODIGO_CONTRATO_AIR = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR

	LEFT JOIN {{ ref('base_faturas_migracao') }} as base_faturas_migracao
		ON base_faturas_migracao.CODIGO_CONTRATO_AIR = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
		AND base_faturas_migracao.mes_referencia_emissao = faturamento_base_arvore_inicial.mes_referencia_emissao

	LEFT JOIN {{ ref('base_faturas_reajustes') }} as base_faturas_reajustes
		ON base_faturas_reajustes.CODIGO_CONTRATO_AIR = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
		AND base_faturas_reajustes.mes_referencia_emissao = faturamento_base_arvore_inicial.mes_referencia_emissao

	LEFT JOIN {{ ref('base_faturas_venda_adicional') }} as base_faturas_venda_adicional
		ON base_faturas_venda_adicional.CODIGO_CONTRATO_AIR = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
		AND base_faturas_venda_adicional.mes_referencia_emissao = faturamento_base_arvore_inicial.mes_referencia_emissao

	LEFT JOIN {{ ref('base_faturas_alteracao_vencimento') }} as base_faturas_alteracao_vencimento
		ON base_faturas_alteracao_vencimento.CODIGO_CONTRATO_AIR = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
		AND base_faturas_alteracao_vencimento.mes_referencia_emissao = faturamento_base_arvore_inicial.mes_referencia_emissao

	LEFT JOIN {{ ref('base_faturas_desconto') }} as base_faturas_desconto
		ON base_faturas_desconto.CODIGO_CONTRATO_AIR = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
		AND base_faturas_desconto.mes_referencia_emissao = faturamento_base_arvore_inicial.mes_referencia_emissao

	LEFT JOIN M1_PAGOS ON M1_PAGOS.CODIGO_CONTRATO_AIR = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR

	LEFT JOIN classificacao_suspensos
		ON faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR = classificacao_suspensos.CODIGO_CONTRATO_AIR

	LEFT JOIN {{ ref('faturamento_tag') }} AS tag
		ON faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
),

cte_tag_tela_3_rn AS (
	SELECT CODIGO_CONTRATO_AIR,
	tag_tela_3,
	ROW_NUMBER() OVER(PARTITION BY CODIGO_CONTRATO_AIR ORDER BY
						CASE
						WHEN tag_tela_3 = 'Suspensão Integral M1' THEN 1
						WHEN tag_tela_3 = 'Suspensão Integral M2' THEN 2
						WHEN tag_tela_3 = 'Suspensão Integral M3+' THEN 3
						WHEN tag_tela_3 = 'Suspensão Parcial - Retorno' THEN 4
						WHEN tag_tela_3 = 'Suspensão Parcial' THEN 5
						WHEN tag_tela_3 = 'Nova Ativação' THEN 6
						WHEN tag_tela_3 = 'Entrante Pós Ciclo' THEN 7
						WHEN tag_tela_3 = 'Multas e Last Bill' THEN 8
						WHEN tag_tela_3 = 'Multas - Equipamento' THEN 9
						WHEN tag_tela_3 = 'Multas - Fidelidade' THEN 10
						WHEN tag_tela_3 = 'Last Bill M1' THEN 11
						WHEN tag_tela_3 = 'Last Bill M0' THEN 12
						WHEN tag_tela_3 = 'Desconto 100%' THEN 13
						WHEN tag_tela_3 = 'Downgrade' THEN 14
						WHEN tag_tela_3 = 'Upgrade' THEN 15
						WHEN tag_tela_3 = 'Migração Compulsória' THEN 16
						WHEN tag_tela_3 = 'Reajuste' THEN 17
						WHEN tag_tela_3 = 'Adição de Produto' THEN 18
						WHEN tag_tela_3 = 'Alteração de Vencimento' THEN 19
						WHEN tag_tela_3 = 'Aplicação de Desconto' THEN 20
						WHEN tag_tela_3 = 'Fim do Desconto' THEN 21
						WHEN tag_tela_3 = 'Duplicado' THEN 22
						WHEN tag_tela_3 = 'sem classificação' THEN 23
						ELSE 24 END) AS rn
	FROM cte_tag_tela_3
)

SELECT CODIGO_CONTRATO_AIR,
	tag_tela_3
FROM cte_tag_tela_3_rn
WHERE rn = 1
