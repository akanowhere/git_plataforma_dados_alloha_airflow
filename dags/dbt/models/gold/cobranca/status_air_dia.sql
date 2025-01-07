SELECT DISTINCT dim_contrato.id_cliente as id_cliente_air,
dim_contrato.id_contrato_air as id_contrato_air,
dim_contrato.status as status_air,
CASE
	WHEN UPPER(COALESCE(U.marca, U1.marca)) = 'SUMICITY' THEN 'Polo Sumicity'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) = 'CLICK' THEN 'Polo Sumicity'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) like '%GIGA%' THEN 'Polo Sumicity'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) like '%UNIVOX%' THEN 'Polo Sumicity'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) like '%VIP%' THEN 'Polo VIP'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) like '%NIU%' THEN 'Polo VIP'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) like '%PAMNET%' THEN 'Polo VIP'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) like '%LIGUE%' THEN 'Polo VIP'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) like '%MOB%' THEN 'Polo Mob'
	WHEN UPPER(COALESCE(U.marca, U1.marca)) IS NULL THEN 'Polo Sumicity'
ELSE NULL
END AS polo,
COALESCE(U.marca, U1.marca, 'SUMICITY') AS marca,
CASE
	WHEN UPPER(COALESCE(U.regional, U1.regional)) = 'REGIAO_06' THEN 'R6'
	WHEN UPPER(COALESCE(U.regional, U1.regional)) = 'REGIAO-07' THEN 'R7'
	WHEN UPPER(COALESCE(U.regional, U1.regional)) IS NULL THEN 'R5'
ELSE COALESCE(U.regional, U1.regional) END AS regional,
CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS dt_atualizacao

FROM {{ ref('dim_contrato') }} AS dim_contrato
LEFT JOIN (
	SELECT E.estado,
			E.cidade,
			E.unidade,
			A.id_contrato_air AS CODIGO_CONTRATO_AIR
	FROM {{ ref('dim_contrato') }} AS A
	INNER JOIN
		(SELECT MAX(id_endereco) AS endereco,
				id_contrato
		FROM {{ ref('dim_contrato_entrega') }} AS dim_contrato_entrega
		GROUP BY id_contrato) AS tb_temp ON tb_temp.id_contrato = A.id_contrato_air
	INNER JOIN {{ ref('dim_endereco') }} AS E ON E.id_endereco = tb_temp.endereco
) AS endereco_contrato
	ON endereco_contrato.CODIGO_CONTRATO_AIR = dim_contrato.id_contrato_air
LEFT JOIN {{ ref('dim_unidade') }} AS U
	ON U.sigla = dim_contrato.unidade_atendimento
	AND UPPER(U.fonte) = 'AIR' AND U.excluido = FALSE
LEFT JOIN {{ ref('dim_unidade') }} AS U1
	ON U.sigla = endereco_contrato.unidade
	AND UPPER(U.fonte) = 'AIR' AND U.excluido = FALSE

UNION

SELECT id_cliente_air,
id_contrato_air,
status_air,
polo,
marca,
regional,
dt_atualizacao

FROM {{ this }}
where date(dt_atualizacao)  >= DATE_SUB(CURRENT_DATE(), 200)
AND date(dt_atualizacao)  < date(CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()))
