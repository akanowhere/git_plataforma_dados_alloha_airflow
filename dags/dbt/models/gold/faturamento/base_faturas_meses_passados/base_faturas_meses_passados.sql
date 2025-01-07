WITH faturas_meses_passados_MES_ANTERIOR AS (
    SELECT DISTINCT B.id_cliente_AIR,
                B.CODIGO_CONTRATO_AIR,
                C.codigo_fatura_sydle,
                C.data_criacao AS emissao_fatura,
                C.data_vencimento AS vencimento_fatura,
                C.forma_pagamento AS forma_pagamento_fatura,
                C.data_pagamento AS data_pagamento_fatura,
                C.mes_referencia AS mes_referencia_fatura,
                C.classificacao AS classificacao_fatura,
                C.status_fatura,
                C.valor_fatura,
                B.mes_referencia_emissao as mes_referencia,
                B.data_ativacao_contrato,
                B.data_cancelamento_contrato,
                B.ciclo,
                CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
	FROM {{ ref('faturamento_base_arvore_inicial') }} as B
	LEFT JOIN {{ ref('faturamento_tag') }} as tag ON B.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
	LEFT JOIN {{ ref('dim_faturas_mailing') }} as C on B.CODIGO_CONTRATO_AIR = C.contrato_air
	WHERE UPPER(tag.tag) = 'SUSPENSO TOTAL'
	AND UPPER({{ translate_column('C.classificacao') }}) IN ('INICIAL', 'FINAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
	AND MONTH(C.data_criacao) = MONTH(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date())))
	AND YEAR(C.data_criacao) = YEAR(DATEADD(MONTH, -1, DATEADD(DAY, -1, current_date())))
	AND UPPER({{ translate_column('C.status_fatura') }}) IN ('BAIXA OPERACIONAL', 'EMITIDA', 'PAGA', 'NEGOCIADA COM O CLIENTE', 'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO')
),

faturas_meses_passados_DOIS_MESES AS (
    SELECT DISTINCT B.id_cliente_AIR,
                B.CODIGO_CONTRATO_AIR,
                C.codigo_fatura_sydle,
                C.data_criacao AS emissao_fatura,
                C.data_vencimento AS vencimento_fatura,
                C.forma_pagamento AS forma_pagamento_fatura,
                C.data_pagamento AS data_pagamento_fatura,
                C.mes_referencia AS mes_referencia_fatura,
                C.classificacao AS classificacao_fatura,
                C.status_fatura,
                C.valor_fatura,
                B.mes_referencia_emissao as mes_referencia,
                B.data_ativacao_contrato,
                B.data_cancelamento_contrato,
                B.ciclo,
                CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
	FROM {{ ref('faturamento_base_arvore_inicial') }} as B
	LEFT JOIN {{ ref('faturamento_tag') }} as tag ON B.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
	LEFT JOIN {{ ref('dim_faturas_mailing') }} as C on B.CODIGO_CONTRATO_AIR = C.contrato_air
	WHERE UPPER(tag.tag) = 'SUSPENSO TOTAL'
	AND UPPER({{ translate_column('C.classificacao') }}) IN ('INICIAL', 'FINAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
	AND MONTH(C.data_criacao) = MONTH(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date())))
	AND YEAR(C.data_criacao) = YEAR(DATEADD(MONTH, -2, DATEADD(DAY, -1, current_date())))
	AND UPPER({{ translate_column('C.status_fatura') }}) IN ('BAIXA OPERACIONAL', 'EMITIDA', 'PAGA', 'NEGOCIADA COM O CLIENTE', 'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO')
),

faturas_meses_passados_TRES_MESES AS (
    SELECT DISTINCT B.id_cliente_AIR,
                B.CODIGO_CONTRATO_AIR,
                C.codigo_fatura_sydle,
                C.data_criacao AS emissao_fatura,
                C.data_vencimento AS vencimento_fatura,
                C.forma_pagamento AS forma_pagamento_fatura,
                C.data_pagamento AS data_pagamento_fatura,
                C.mes_referencia AS mes_referencia_fatura,
                C.classificacao AS classificacao_fatura,
                C.status_fatura,
                C.valor_fatura,
                B.mes_referencia_emissao as mes_referencia,
                B.data_ativacao_contrato,
                B.data_cancelamento_contrato,
                B.ciclo,
                CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
	FROM {{ ref('faturamento_base_arvore_inicial') }} as B
	LEFT JOIN {{ ref('faturamento_tag') }} as tag ON B.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
	LEFT JOIN {{ ref('dim_faturas_mailing') }} as C on B.CODIGO_CONTRATO_AIR = C.contrato_air
	WHERE UPPER(tag.tag) = 'SUSPENSO TOTAL'
	AND UPPER({{ translate_column('C.classificacao') }}) IN ('INICIAL', 'FINAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
	AND MONTH(C.data_criacao) = MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date())))
	AND YEAR(C.data_criacao) = YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date())))
	AND UPPER({{ translate_column('C.status_fatura') }}) IN ('BAIXA OPERACIONAL', 'EMITIDA', 'PAGA', 'NEGOCIADA COM O CLIENTE', 'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO')
),

faturas_meses_passados_OUTROS_MESES AS (
    SELECT DISTINCT B.id_cliente_AIR,
                B.CODIGO_CONTRATO_AIR,
                C.codigo_fatura_sydle,
                C.data_criacao AS emissao_fatura,
                C.data_vencimento AS vencimento_fatura,
                C.forma_pagamento AS forma_pagamento_fatura,
                C.data_pagamento AS data_pagamento_fatura,
                C.mes_referencia AS mes_referencia_fatura,
                C.classificacao AS classificacao_fatura,
                C.status_fatura,
                C.valor_fatura,
                B.mes_referencia_emissao as mes_referencia,
                B.data_ativacao_contrato,
                B.data_cancelamento_contrato,
                B.ciclo,
                CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
	FROM {{ ref('faturamento_base_arvore_inicial') }} as B
	LEFT JOIN {{ ref('faturamento_tag') }} as tag ON B.CODIGO_CONTRATO_AIR = tag.CODIGO_CONTRATO_AIR
	LEFT JOIN {{ ref('dim_faturas_mailing') }} as C on B.CODIGO_CONTRATO_AIR = C.contrato_air
	WHERE UPPER(tag.tag) = 'SUSPENSO TOTAL'
	AND B.CODIGO_CONTRATO_AIR NOT IN (SELECT CODIGO_CONTRATO_AIR FROM faturas_meses_passados_MES_ANTERIOR)
	AND B.CODIGO_CONTRATO_AIR NOT IN (SELECT CODIGO_CONTRATO_AIR FROM faturas_meses_passados_DOIS_MESES)
	AND B.CODIGO_CONTRATO_AIR NOT IN (SELECT CODIGO_CONTRATO_AIR FROM faturas_meses_passados_TRES_MESES)
	AND UPPER({{ translate_column('C.classificacao') }}) IN ('INICIAL', 'FINAL', 'PERIODICO', 'REFATURAMENTO', 'MANUAL', 'PEDIDO')
	AND CAST(C.data_criacao AS DATE) < CAST((CONCAT(YEAR(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '-', MONTH(DATEADD(MONTH, -3, DATEADD(DAY, -1, current_date()))), '-01'))AS DATE)
	AND UPPER({{ translate_column('C.status_fatura') }}) IN ('BAIXA OPERACIONAL', 'EMITIDA', 'PAGA', 'NEGOCIADA COM O CLIENTE', 'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO')
),

uniao_meses_passados AS (
    SELECT *
    FROM faturas_meses_passados_MES_ANTERIOR

    UNION

    SELECT *
    FROM faturas_meses_passados_DOIS_MESES

    UNION

    SELECT *
    FROM faturas_meses_passados_TRES_MESES

    UNION

    SELECT *
    FROM faturas_meses_passados_OUTROS_MESES
)

SELECT id_cliente_AIR,
        CODIGO_CONTRATO_AIR,
        codigo_fatura_sydle,
        emissao_fatura,
        vencimento_fatura,
        forma_pagamento_fatura,
        data_pagamento_fatura,
        mes_referencia_fatura,
        classificacao_fatura,
        status_fatura,
        valor_fatura,
        mes_referencia,
        data_ativacao_contrato,
        try_cast(data_cancelamento_contrato as string) as data_cancelamento_contrato,
        ciclo,
        data_extracao
FROM uniao_meses_passados

union

SELECT id_cliente_AIR,
        CODIGO_CONTRATO_AIR,
        codigo_fatura_sydle,
        emissao_fatura,
        vencimento_fatura,
        forma_pagamento_fatura,
        data_pagamento_fatura,
        mes_referencia_fatura,
        classificacao_fatura,
        status_fatura,
        valor_fatura,
        mes_referencia,
        data_ativacao_contrato,
        data_cancelamento_contrato,
        ciclo,
        data_extracao
FROM {{ this }}
WHERE mes_referencia <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
