WITH dados_contrato AS (
    SELECT DISTINCT
        b.id_contrato_air,
        b.status as status_contrato,
        CASE
            WHEN b.b2b = TRUE THEN 'B2B'
            WHEN b.pme = TRUE THEN 'PME'
            ELSE 'B2C'
        END AS segmento,
        b.id_cliente as id_cliente_air,
        CASE
            WHEN b.b2b = TRUE AND UPPER(c.`grupo`) = 'GC_PODER_PUBLICO' THEN 'JURIDICO-PUBLICO'
            WHEN b.b2b = TRUE THEN 'JURIDICO'
            WHEN b.pme = TRUE AND UPPER(c.`grupo`) = 'GC_PODER_PUBLICO' THEN 'FISICO-PUBLICO'
            WHEN b.pme = TRUE THEN 'FISICO'
            ELSE 'FISICO'
        END AS tipo_pessoa,
        CASE
            WHEN cpf IS NOT NULL THEN cpf
            ELSE cnpj
        END AS cpf_cnpj,
        b.data_cancelamento
    FROM {{ ref('dim_contrato') }}  b
    LEFT JOIN {{ ref('dim_cliente') }}  c
        ON b.id_cliente = c.id_cliente AND UPPER(c.fonte) = 'AIR'
),

dados_endereco AS (
    SELECT DISTINCT
        B.id_contrato_air,
        CASE
            WHEN UPPER(U.marca) like 'SUMICITY' THEN 'Polo Sumicity'
            WHEN UPPER(U.marca) like 'CLICK' THEN 'Polo Sumicity'
            WHEN UPPER(U.marca) like '%GIGA%' THEN 'Polo Sumicity'
            WHEN UPPER(U.marca) like '%UNIVOX%' THEN 'Polo Sumicity'
            WHEN UPPER(U.marca) like '%VIP%' THEN 'Polo VIP'
            WHEN UPPER(U.marca) like '%NIU%' THEN 'Polo VIP'
            WHEN UPPER(U.marca) like '%PAMNET%' THEN 'Polo VIP'
            WHEN UPPER(U.marca) like '%LIGUE%' THEN 'Polo VIP'
            WHEN UPPER(U.marca) like '%MOB%' THEN 'Polo Mob'
        ELSE NULL
        END AS polo,
        U.marca,
        REPLACE(REPLACE(UPPER({{ translate_column('endereco_contrato.cidade') }}), CHAR(13), ''), CHAR(10), '') as cidade,
        UPPER(endereco_contrato.estado) AS UF,
        UPPER(subregionais.macro_regional) AS Regiao,
        UPPER(subregionais.sigla_regional) AS regional,
        UPPER(subregionais.subregional) AS sub_regional
    FROM
        {{ ref('dim_contrato') }} B
    LEFT JOIN (
        SELECT
            E.estado,
            E.cidade,
            E.unidade,
            A.id_contrato_air as CODIGO_CONTRATO_AIR
        FROM
            {{ ref('dim_contrato') }} A
        INNER JOIN (
            SELECT
                MAX(A.id_endereco) AS endereco,
                id_contrato
            FROM
                {{ ref('dim_contrato_entrega') }} A
            GROUP BY
                id_contrato
        ) AS tb_temp
        ON tb_temp.id_contrato = A.id_contrato_air
        INNER JOIN
            {{ ref('dim_endereco') }} E
        ON E.id_endereco = tb_temp.endereco
    ) AS endereco_contrato
    ON endereco_contrato.CODIGO_CONTRATO_AIR = B.id_contrato_air
    LEFT JOIN
        {{ ref('dim_unidade') }} U
    ON U.sigla = COALESCE(endereco_contrato.unidade, B.unidade_atendimento)
    AND U.fonte = 'AIR'
    LEFT JOIN
        {{ get_catalogo('silver') }}.stage_seeds_data.subregionais
    ON UPPER({{ translate_column('endereco_contrato.cidade') }}) = UPPER(subregionais.cidade_sem_acento)
    AND UPPER(endereco_contrato.estado) = UPPER(subregionais.uf)
),

-----------------------------------------
----DADOS CHAMADOS ABERTOS QUE PODEM TER FATURAS CONTESTADAS
-----------------------------------------
chamados_descontos_cobranca AS (
    SELECT codigo_contrato
        ,data_conclusao

    FROM {{ ref('dim_chamado') }}
    WHERE YEAR(data_conclusao) = YEAR(DATEADD(DAY, -1, DATEADD(HOUR, -3, current_date())))
    AND MONTH(data_conclusao) = MONTH(DATEADD(DAY, -1, DATEADD(HOUR, -3, current_date())))

    AND UPPER(nome_fila) in ('FINANCEIRO - CONTESTAÇÃO DE FATURA'
                    ,'FINANCEIRO -OUVIDORIA/JURIDICO'
                    ,'FINANCEIRO - DIAS SEM CONEXÃO DESCONTO (PROBLEMA TÉCNICO)'
                    ,'FINANCEIRO - PAGAMENTO TROCADO'
                    ,'FINANCEIRO - DESCONTO RETENÇÃO'
                    ,'FINANCEIRO - PAGAMENTO EM DUPLICIDADE'
                    ,'FINANCEIRO - DESCONTO PROMOCIONAL NÃO INTEGRADO'
                    ,'FINANCEIRO - ANÁLISE DE FRAUDE'
                    ,'FINANCEIRO - VALOR PROPORCIONAL -TROCA DE PLANO'
                    ,'FINANCEIRO - CORTESIAS'
                    ,'FINANCEIRO - PLANO NÃO INTEGRADO'
                    ,'FINANCEIRO - REMOÇÃO FATURA DE EQUIPAMENTO')
        AND UPPER(motivo_conclusao) IN ('COBRANÇA A MAIOR'
                            ,'COBRANÇA A MENOR'
                            ,'COBRANÇA DUPLICADA'
                            ,'COBRANÇA EXTRA'
                            ,'DESCONTO NÃO INTEGRADO'
                            ,'FATURA MÍNIMA'
                            ,'PLANO NÃO INTEGRADO'
                            ,'PROPORCIONAL(TROCA DE PLANO)'
                            ,'PAGAMENTO EM DUPLICIDADE'
                            ,'PAGAMENTO TROCADO'
                            ,'REMOÇÃO DE DÉBITO POR FRAUDE'
                            ,'REMOÇÃO VIA CERTIDÃO DE ÓBITO'
                            ,'CORTESIA RENOVADA'
                            ,'DESCONTO AJUSTADO'
                            ,'DESCONTO RETANÇÃO'
                            ,'OUVIDORIA/JURÍDICO')
),

-----------------------------------------
----FATURAS QUE TEM O CONTRATO EM QUE FOI ABERTO CHAMADO NO MÊS E SÃO DO TIPO REFATURAMENTO
-----------------------------------------

faturas_refaturadas AS (
    select DISTINCT A.contrato_air,
    A.codigo_fatura_sydle,
    A.classificacao AS classificacao_fatura,
    A.data_vencimento,
    A.status_fatura,
    A.valor_fatura,
    A.data_pagamento,
    A.valor_pago,
    A.data_criacao

    FROM {{ ref('dim_faturas_mailing') }} A
    INNER JOIN chamados_descontos_cobranca B
    ON A.contrato_air = B.codigo_contrato
    AND CAST(A.data_criacao AS DATE) = CAST(B.data_conclusao AS DATE)
    where UPPER(classificacao) = 'REFATURAMENTO' AND UPPER(A.status_fatura) <> 'CANCELADA'
),

-----------------------------------------
----FATURAS QUE TEM O CONTRATO EM QUE FOI ABERTO CHAMADO NO MÊS E ESTÃO COM STATUS CANCELADA
-----------------------------------------

faturas_canceladas AS (
    select DISTINCT A.contrato_air,
    A.codigo_fatura_sydle,
    A.classificacao AS classificacao_fatura,
    A.data_vencimento,
    A.status_fatura,
    A.valor_fatura,
    A.data_criacao

    FROM {{ ref('dim_faturas_mailing') }} A
    INNER JOIN chamados_descontos_cobranca B
    ON A.contrato_air = B.codigo_contrato
    where UPPER(A.status_fatura) = 'CANCELADA' AND CAST(A.data_atualizacao AS DATE) >= CAST(B.data_conclusao AS DATE)
),

faturas_refaturadas_rn AS (
	SELECT A.contrato_air,
    A.codigo_fatura_sydle,
    A.classificacao_fatura,
    A.data_vencimento,
    A.status_fatura,
    A.valor_fatura,
    A.data_pagamento,
    A.valor_pago,
    A.data_criacao,
    CASE
        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
    END AS mes_referencia,
	ROW_NUMBER() OVER (PARTITION BY A.contrato_air ORDER BY A.data_criacao ASC) AS RN
	FROM faturas_refaturadas A
),

faturas_canceladas_rn AS (
	SELECT A.contrato_air,
    A.codigo_fatura_sydle,
    A.classificacao_fatura,
    A.data_vencimento,
    A.status_fatura,
    A.valor_fatura,
    A.data_criacao,
	ROW_NUMBER() OVER (PARTITION BY A.contrato_air ORDER BY A.data_criacao ASC) AS RN
	FROM faturas_canceladas A
)

SELECT
A.mes_referencia,
A.contrato_air,
D.segmento,
C.polo,
C.marca,
C.cidade,
C.UF,
C.Regiao,
C.regional,
C.sub_regional,
A.codigo_fatura_sydle AS codigo_fatura_sydle_refaturada,
A.classificacao_fatura AS classificacao_fatura_refaturada,
CAST(A.data_vencimento AS DATE) AS data_vencimento_fatura_refaturada,
A.status_fatura AS status_fatura_refaturada,
A.valor_fatura AS valor_fatura_refaturada,
A.data_pagamento AS data_pagamento_fatura_refaturada,
A.valor_pago AS valor_pago_fatura_refaturada,

B.codigo_fatura_sydle AS codigo_fatura_sydle_cancelada,
B.classificacao_fatura AS classificacao_fatura_cancelada,
CAST(B.data_vencimento AS DATE) AS data_vencimento_fatura_cancelada,
B.status_fatura AS status_fatura_cancelada,
B.valor_fatura AS valor_fatura_cancelada,
CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM faturas_refaturadas_rn AS A
LEFT JOIN faturas_canceladas_rn AS B
	ON A.contrato_air = B.contrato_air
	AND A.RN = B.RN
LEFT JOIN dados_endereco C
	ON A.contrato_air = C.id_contrato_air
LEFT JOIN dados_contrato D
	ON A.contrato_air = D.id_contrato_air

union

SELECT
mes_referencia,
contrato_air,
segmento,
polo,
marca,
cidade,
UF,
Regiao,
regional,
sub_regional,
codigo_fatura_sydle_refaturada,
classificacao_fatura_refaturada,
data_vencimento_fatura_refaturada,
status_fatura_refaturada,
valor_fatura_refaturada,
data_pagamento_fatura_refaturada,
valor_pago_fatura_refaturada,
codigo_fatura_sydle_cancelada,
classificacao_fatura_cancelada,
data_vencimento_fatura_cancelada,
status_fatura_cancelada,
valor_fatura_cancelada,
data_extracao
FROM {{ this }}
where mes_referencia <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
