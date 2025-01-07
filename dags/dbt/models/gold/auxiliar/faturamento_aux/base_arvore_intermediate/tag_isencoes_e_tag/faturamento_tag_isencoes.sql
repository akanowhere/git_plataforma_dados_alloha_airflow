WITH cte AS (
	SELECT DISTINCT faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR,
	CASE
		WHEN ((codigo_fatura_sydle IS NULL AND (UPPER(dim_contrato.marcador) LIKE '%PERMUTA%' OR UPPER(dim_contrato.marcador) LIKE '%CORTESIA%'))
			OR (codigo_fatura_sydle IS NULL AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) like '%PERMUTA%'))
		THEN 'cortesias e permutas'
		WHEN codigo_fatura_sydle IS NULL AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) not like '%PERMUTA%'
		THEN 'colaborador alloha'
		WHEN codigo_fatura_sydle IS NULL AND porcentagem_desconto = '100'
		THEN 'Desconto 100%'
	ELSE NULL END AS tag_isencoes,
	1 AS isencoes

	FROM {{ ref('faturamento_base_arvore_inicial') }} as faturamento_base_arvore_inicial
	LEFT JOIN {{ ref('dim_contrato') }} as dim_contrato
	ON dim_contrato.id_contrato_air = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR
	LEFT JOIN {{ ref('faturamento_air_contrato_campanha') }} as faturamento_air_contrato_campanha
		on faturamento_air_contrato_campanha.id_contrato = faturamento_base_arvore_inicial.CODIGO_CONTRATO_AIR

	WHERE ((codigo_fatura_sydle IS NULL AND (UPPER(dim_contrato.marcador) LIKE '%PERMUTA%' OR UPPER(dim_contrato.marcador) LIKE '%CORTESIA%'))
	OR (codigo_fatura_sydle IS NULL AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) like '%PERMUTA%')
	OR (codigo_fatura_sydle IS NULL AND faturamento_air_contrato_campanha.id_contrato is not null and UPPER(faturamento_air_contrato_campanha.nome_campanha) not like '%PERMUTA%')
	OR (codigo_fatura_sydle IS NULL AND porcentagem_desconto = '100'))
),

cte_rn AS (
	SELECT CODIGO_CONTRATO_AIR,
	tag_isencoes,
	isencoes,
	ROW_NUMBER() OVER(PARTITION BY CODIGO_CONTRATO_AIR ORDER BY
						CASE
						WHEN tag_isencoes = 'cortesias e permutas' then 1
						WHEN tag_isencoes = 'colaborador alloha' then 2
						WHEN tag_isencoes = 'colaborador alloha' then 3
						WHEN tag_isencoes = 'Desconto 100%' then 4
						ELSE 5 END) AS rn
	FROM cte
)

SELECT CODIGO_CONTRATO_AIR,
tag_isencoes,
isencoes
FROM cte_rn
WHERE rn = 1
