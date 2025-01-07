WITH fatura_inicial as (
	SELECT CODIGO_CONTRATO_AIR, mes_referencia_emissao, MIN(codigo_fatura_sydle) AS codigo_fatura_sydle,
	concat(CODIGO_CONTRATO_AIR, '-', mes_referencia_emissao) as contrato_e_mes_referencia
	FROM {{ ref('base_faturas') }}
	WHERE codigo_fatura_sydle IS NOT NULL
	AND ((UPPER(classificacao_fatura) = 'INICIAL') OR (UPPER(classificacao_fatura) = 'PEDIDO'))
    AND base_faturas.mes_referencia_emissao =
	CASE
		WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
		ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
	END
	GROUP BY CODIGO_CONTRATO_AIR, mes_referencia_emissao
),

fatura_final as (
	SELECT CODIGO_CONTRATO_AIR, mes_referencia_emissao, MIN(codigo_fatura_sydle) AS codigo_fatura_sydle,
	concat(CODIGO_CONTRATO_AIR, '-', mes_referencia_emissao) as contrato_e_mes_referencia
	FROM {{ ref('base_faturas') }}
	WHERE codigo_fatura_sydle IS NOT NULL
	AND UPPER(classificacao_fatura) = 'FINAL'
    AND base_faturas.mes_referencia_emissao =
	CASE
		WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
		ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
	END
	GROUP BY CODIGO_CONTRATO_AIR, mes_referencia_emissao
),

fatura_periodica as (
	SELECT CODIGO_CONTRATO_AIR, mes_referencia_emissao, MIN(codigo_fatura_sydle) AS codigo_fatura_sydle,
	concat(CODIGO_CONTRATO_AIR, '-', mes_referencia_emissao) as contrato_e_mes_referencia
	FROM {{ ref('base_faturas') }}
	WHERE codigo_fatura_sydle IS NOT NULL
	AND UPPER({{ translate_column('classificacao_fatura') }}) = 'PERIODICO'
    AND base_faturas.mes_referencia_emissao =
	CASE
		WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
		ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
	END
	GROUP BY CODIGO_CONTRATO_AIR, mes_referencia_emissao
),

fatura_refaturamento as (
	SELECT CODIGO_CONTRATO_AIR, mes_referencia_emissao, MIN(codigo_fatura_sydle) AS codigo_fatura_sydle,
	concat(CODIGO_CONTRATO_AIR, '-', mes_referencia_emissao) as contrato_e_mes_referencia
	FROM {{ ref('base_faturas') }}
	WHERE codigo_fatura_sydle IS NOT NULL
	AND UPPER(classificacao_fatura) = 'REFATURAMENTO'
    AND base_faturas.mes_referencia_emissao =
	CASE
		WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
		ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
	END
	GROUP BY CODIGO_CONTRATO_AIR, mes_referencia_emissao
),

fatura_manual as (
	SELECT CODIGO_CONTRATO_AIR, mes_referencia_emissao, MIN(codigo_fatura_sydle) AS codigo_fatura_sydle,
	concat(CODIGO_CONTRATO_AIR, '-', mes_referencia_emissao) as contrato_e_mes_referencia
	FROM {{ ref('base_faturas') }}
	WHERE codigo_fatura_sydle IS NOT NULL
	AND UPPER(classificacao_fatura) = 'MANUAL'
    AND base_faturas.mes_referencia_emissao =
	CASE
		WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
		ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
	END
	GROUP BY CODIGO_CONTRATO_AIR, mes_referencia_emissao
),

nao_faturado as (
	SELECT CODIGO_CONTRATO_AIR, mes_referencia_emissao
	FROM {{ ref('base_faturas') }}
	WHERE codigo_fatura_sydle IS NULL
    AND base_faturas.mes_referencia_emissao =
	CASE
		WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
		ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
	END
	GROUP BY CODIGO_CONTRATO_AIR, mes_referencia_emissao
),

faturados AS (

		SELECT *
		from fatura_inicial

		UNION

		SELECT *
		from fatura_final
		WHERE fatura_final.contrato_e_mes_referencia NOT IN
				(select fatura_inicial.contrato_e_mes_referencia from fatura_inicial)

		UNION

		SELECT *
		from fatura_periodica
		WHERE fatura_periodica.contrato_e_mes_referencia NOT IN
				(select fatura_inicial.contrato_e_mes_referencia from fatura_inicial)
			AND fatura_periodica.contrato_e_mes_referencia NOT IN
				(select fatura_final.contrato_e_mes_referencia from fatura_final)

		UNION

		SELECT *
		from fatura_refaturamento
		WHERE fatura_refaturamento.contrato_e_mes_referencia NOT IN
				(select fatura_inicial.contrato_e_mes_referencia from fatura_inicial)
			AND fatura_refaturamento.contrato_e_mes_referencia NOT IN
				(select fatura_final.contrato_e_mes_referencia from fatura_final)
			AND fatura_refaturamento.contrato_e_mes_referencia NOT IN
				(select fatura_periodica.contrato_e_mes_referencia from fatura_periodica)

		UNION

		SELECT *
		from fatura_manual
		WHERE fatura_manual.contrato_e_mes_referencia NOT IN
				(select fatura_inicial.contrato_e_mes_referencia from fatura_inicial)
			AND fatura_manual.contrato_e_mes_referencia NOT IN
				(select fatura_final.contrato_e_mes_referencia from fatura_final)
			AND fatura_manual.contrato_e_mes_referencia NOT IN
				(select fatura_periodica.contrato_e_mes_referencia from fatura_periodica)
			AND fatura_manual.contrato_e_mes_referencia NOT IN
				(select fatura_refaturamento.contrato_e_mes_referencia from fatura_refaturamento)
)

SELECT DISTINCT base_faturas.*
FROM {{ ref('base_faturas') }}
INNER JOIN faturados
    ON faturados.CODIGO_CONTRATO_AIR = base_faturas.CODIGO_CONTRATO_AIR
    AND faturados.mes_referencia_emissao = base_faturas.mes_referencia_emissao
    and faturados.codigo_fatura_sydle = base_faturas.codigo_fatura_sydle

UNION

SELECT DISTINCT base_faturas.*
FROM {{ ref('base_faturas') }}
inner join nao_faturado
    ON nao_faturado.CODIGO_CONTRATO_AIR = base_faturas.CODIGO_CONTRATO_AIR
    AND nao_faturado.mes_referencia_emissao = base_faturas.mes_referencia_emissao
