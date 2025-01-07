WITH perfil_clientes_rn AS (
	SELECT contrato_air
		,mes_referencia
		,status_contrato
		,segmento
		,id_cliente_air
		,tipo_pessoa
		,polo
		,marca
		,cidade
		,UF
		,Regiao
		,regional
		,sub_regional
		,codigo_fatura_sydle
		,data_criacao
		,data_vencimento
		,status_fatura
		,data_pagamento
		,forma_pagamento
		,classificacao_fatura
		,valor_fatura
		,valor_pago
		,aging_pagamento
		,comportamento_mensal
		,ROW_NUMBER() OVER (PARTITION BY contrato_air ORDER BY
													CASE
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'PERIODICO' AND data_pagamento IS NOT NULL THEN 1
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'PERIODICO' THEN 2
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'MANUAL' AND data_pagamento IS NOT NULL THEN 3
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'MANUAL' THEN 4
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'REFATURAMENTO' AND data_pagamento IS NOT NULL THEN 5
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'REFATURAMENTO' THEN 6
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'NEGOCIACAO' AND data_pagamento IS NOT NULL THEN 7
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'NEGOCIACAO' THEN 8
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'INICIAL' AND data_pagamento IS NOT NULL THEN 9
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'INICIAL' THEN 10
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'FINAL' AND data_pagamento IS NOT NULL THEN 11
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'FINAL' THEN 12
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'PEDIDO' AND data_pagamento IS NOT NULL THEN 13
														WHEN UPPER({{ translate_column('classificacao_fatura') }}) = 'PEDIDO' THEN 14
													ELSE 15 END ASC, data_criacao ASC) AS RN
	FROM {{ ref('perfil_clientes_analitico') }}
	WHERE mes_referencia = --'06/2024'
	CASE
		WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
		THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
		ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
	END
	AND contrato_air is not null
	AND valor_fatura > '0'
	AND UPPER(sistema) = 'SYDLE'
	AND UPPER({{ translate_column('status_fatura') }}) NOT IN ('NEGOCIADA COM O CLIENTE',
																'FATURA MINIMA',
																'ABONADA',
																'CANCELADA')
),

filtro_rn AS (
	SELECT contrato_air
			,mes_referencia
			,status_contrato
			,segmento
			,id_cliente_air
			,tipo_pessoa
			,polo
			,marca
			,cidade
			,UF
			,Regiao
			,regional
			,sub_regional
			,codigo_fatura_sydle
			,data_criacao
			,data_vencimento
			,status_fatura
			,data_pagamento
			,forma_pagamento
			,classificacao_fatura
			,valor_fatura
			,valor_pago
			,aging_pagamento
			,comportamento_mensal
			,NULL AS perfil_recorrente

	FROM perfil_clientes_rn AS A
	WHERE A.RN = 1
),

-----------------------------------------
----SCRIPT PEGANDO OS ULTIMOS 6 MESES DE COMPORTAMENTO MENSAL E VERIFICANDO A QUANTIDADE DE REPETIÇÕES DO MESMO COMPORTAMENTO MENSAL POR CONTRATO.
-----------------------------------------

perfil_recorrente AS (
SELECT contrato_air, comportamento_mensal, COUNT(comportamento_mensal) qtd

FROM {{ this }}
where mes_referencia in (
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))))
END
)
group by contrato_air, comportamento_mensal
ORDER BY contrato_air
),


-----------------------------------------
----SCRIPT PARA PEGAR O PIOR PERFIL DO CONTRATO NOS ULTIMOS 6 MESES, PARA PREENCHER OS QUE NÃO SE ENQUADRARAM NAS REGRAS ANTERIORES.
-----------------------------------------

pior_perfil AS (
SELECT contrato_air, comportamento_mensal,
ROW_NUMBER() OVER (PARTITION BY contrato_air ORDER BY
													CASE
														WHEN comportamento_mensal = 'ALTO RISCO' THEN 1
														WHEN comportamento_mensal = 'MEDIO RISCO' THEN 2
														WHEN comportamento_mensal = 'BAIXO RISCO' THEN 3
														WHEN comportamento_mensal = 'BOM PAGADOR' THEN 4
													ELSE 5 END ASC) AS RN

FROM {{ this }}
where mes_referencia in (
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -4, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -5, DATEADD(DAY, -1, current_date()))))
END,
CASE
    WHEN LEN(CONCAT(MONTH(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))))) = 6 
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date())))))
    ELSE CONCAT(MONTH(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))), '/', YEAR(DATEADD(MONTH, -6, DATEADD(DAY, -1, current_date()))))
END
)
group by contrato_air, comportamento_mensal
ORDER BY contrato_air
),

pior_perfil_rn AS (
    SELECT contrato_air, comportamento_mensal
    FROM pior_perfil
    WHERE RN = 1
),

-----------------------------------------
----ELEGER APENAS 1 FATURA POR CONTRATO E CRIAR A TABELA DE PERFIL DE CLIENTES, COM CONTRATOS ÚNICOS POR MÊS REFERÊNCIA.
-----------------------------------------
perfil_recorrente_rn AS (
SELECT DISTINCT A.contrato_air
		,mes_referencia
		,status_contrato
		,segmento
		,id_cliente_air
		,tipo_pessoa
		,polo
		,marca
		,cidade
		,UF
		,Regiao
		,regional
		,sub_regional
		,codigo_fatura_sydle
		,data_criacao
		,data_vencimento
		,status_fatura
		,data_pagamento
		,forma_pagamento
		,classificacao_fatura
		,valor_fatura
		,valor_pago
		,aging_pagamento
		,A.comportamento_mensal
        ,CASE
            WHEN perfil_recorrente.comportamento_mensal = 'BOM PAGADOR' AND perfil_recorrente.qtd >= 5 THEN 'BOM PAGADOR'
            WHEN ((perfil_recorrente.comportamento_mensal = 'BOM PAGADOR' AND perfil_recorrente.qtd >= 4)
                OR ((perfil_recorrente.comportamento_mensal = 'BOM PAGADOR' AND perfil_recorrente.qtd = 3)
                    AND (perfil_recorrente.comportamento_mensal = 'BAIXO RISCO' AND perfil_recorrente.qtd >= 1)
                    AND (perfil_recorrente.comportamento_mensal <> 'ALTO RISCO'))
                OR (perfil_recorrente.comportamento_mensal = 'BAIXO RISCO' AND perfil_recorrente.qtd >= 4)) THEN 'BAIXO RISCO'
            WHEN ((perfil_recorrente.comportamento_mensal = 'BOM PAGADOR' AND perfil_recorrente.qtd = 3
                AND perfil_recorrente.comportamento_mensal = 'MEDIO RISCO' AND perfil_recorrente.qtd >= 1
                AND perfil_recorrente.comportamento_mensal <> 'ALTO RISCO')
                OR (perfil_recorrente.comportamento_mensal = 'MEDIO RISCO' AND perfil_recorrente.qtd >= 4)) THEN 'MEDIO RISCO'
            WHEN ((perfil_recorrente.comportamento_mensal = 'BOM PAGADOR' AND perfil_recorrente.qtd = 3
                AND perfil_recorrente.comportamento_mensal = 'ALTO RISCO' AND perfil_recorrente.qtd >= 1)
                OR (perfil_recorrente.comportamento_mensal = 'MEDIO RISCO' AND perfil_recorrente.qtd >= 4)) THEN 'ALTO RISCO'
            WHEN pior_perfil_rn.contrato_air is not null THEN pior_perfil_rn.comportamento_mensal
            ELSE A.comportamento_mensal
        END AS perfil_recorrente
FROM filtro_rn AS A
LEFT JOIN perfil_recorrente ON A.contrato_air = perfil_recorrente.contrato_air
LEFT JOIN pior_perfil_rn ON A.contrato_air = pior_perfil_rn.contrato_air
),

final AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY contrato_air ORDER BY CASE
                                                        WHEN perfil_recorrente = 'BOM PAGADOR' THEN 1
                                                        WHEN perfil_recorrente = 'BAIXO RISCO' THEN 2
                                                        WHEN perfil_recorrente = 'MEDIO RISCO' THEN 3
                                                        WHEN perfil_recorrente = 'ALTO RISCO' THEN 4
                                                        ELSE 5 END) AS rn
FROM perfil_recorrente_rn
)

select contrato_air
		,mes_referencia
		,status_contrato
		,segmento
		,id_cliente_air
		,tipo_pessoa
		,polo
		,marca
		,cidade
		,UF
		,Regiao
		,regional
		,sub_regional
		,codigo_fatura_sydle
		,data_criacao
		,data_vencimento
		,status_fatura
		,data_pagamento
		,forma_pagamento
		,classificacao_fatura
		,valor_fatura
		,valor_pago
		,aging_pagamento
		,comportamento_mensal
    	,perfil_recorrente
		,CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
from final
WHERE rn = 1

UNION

SELECT  contrato_air
		,mes_referencia
		,status_contrato
		,segmento
		,id_cliente_air
		,tipo_pessoa
		,polo
		,marca
		,cidade
		,UF
		,Regiao
		,regional
		,sub_regional
		,codigo_fatura_sydle
		,data_criacao
		,data_vencimento
		,status_fatura
		,data_pagamento
		,forma_pagamento
		,classificacao_fatura
		,valor_fatura
		,valor_pago
		,aging_pagamento
		,comportamento_mensal
        ,perfil_recorrente
		,data_extracao
FROM {{ this }}
WHERE mes_referencia <> --'06/2024'
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
