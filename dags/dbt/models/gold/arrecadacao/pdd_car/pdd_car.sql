{{ config(
    materialized='table',
    liquid_clustered_by=['id_cliente_air', 'mes_referencia']
) }}

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

dados_faturas_PDD AS (
    SELECT DISTINCT A.contrato_air,
    CASE
        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
    END AS mes_referencia,
    A.codigo_fatura_sydle,
    A.data_vencimento,
    A.status_fatura,
    A.forma_pagamento,
    A.classificacao AS classificacao_fatura,
    A.valor_fatura

    FROM {{ ref('dim_faturas_mailing') }} A

    where ((UPPER(A.classificacao) <> 'FINAL')
        OR (UPPER(A.classificacao) = 'FINAL' and UPPER(A.marca) = 'SUMICITY' AND CAST(A.data_criacao AS DATE) < '2020-06-01'))
    AND UPPER({{ translate_column('A.status_fatura') }}) NOT IN ('PAGA',
                                                                'NEGOCIADA COM O CLIENTE',
                                                                'FATURA MINIMA',
                                                                'ABONADA',
                                                                'CANCELADA')
    AND DATE(A.data_criacao) < DATE(CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()))
    AND A.codigo_fatura_sydle not in (
        SELECT codigo_fatura_sydle
        FROM {{ get_catalogo('bronze') }}.legado.expurgo_pdd
    )
),

query_final AS (
    SELECT DISTINCT A.contrato_air,
    A.mes_referencia,
    B.status_contrato,
    B.segmento,
    B.id_cliente_air,
    B.tipo_pessoa,
    C.polo,
    C.marca,
    C.cidade,
    C.UF,
    C.Regiao,
    C.regional,
    C.sub_regional,
    A.codigo_fatura_sydle,
    A.data_vencimento,
    A.status_fatura,
    A.classificacao_fatura,
    A.valor_fatura,
    DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) AS aging,
    CASE
        WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) > 90 THEN 'PDD'
    ELSE NULL END AS classificacao_pdd,
    CASE
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) <= 0 THEN 'A VENCER'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 1 AND 30 THEN 'DE 1 A 30 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 31 AND 60 THEN 'DE 31 A 60 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 61 AND 90 THEN 'DE 61 A 90 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 91 AND 120 THEN 'DE 91 A 120 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 121 AND 150 THEN 'DE 121 A 150 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 151 AND 180 THEN 'DE 151 A 180 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 181 AND 210 THEN 'DE 181 A 210 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 211 AND 240 THEN 'DE 211 A 240 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 241 AND 270 THEN 'DE 241 A 270 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 271 AND 300 THEN 'DE 271 A 300 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 301 AND 330 THEN 'DE 301 A 330 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) BETWEEN 331 AND 365 THEN 'DE 331 A 365 DIAS'
    WHEN DATEDIFF(DAY, A.data_vencimento, DATEADD(HOUR, -3, current_timestamp())) > 365 THEN 'MAIOR QUE 365 DIAS'
    ELSE NULL END as classificacao_car,
    A.forma_pagamento

    FROM dados_faturas_PDD A
    LEFT JOIN dados_contrato B
        ON A.contrato_air = B.id_contrato_air
    LEFT JOIN dados_endereco C
        ON A.contrato_air = C.id_contrato_air
),

pdd_arrasto AS (
    SELECT DISTINCT id_cliente_air
    FROM query_final
    WHERE classificacao_pdd = 'PDD'
)

SELECT contrato_air,
    mes_referencia,
    status_contrato,
    segmento,
    query_final.id_cliente_air,
    tipo_pessoa,
    polo,
    marca,
    cidade,
    UF,
    Regiao,
    regional,
    sub_regional,
    codigo_fatura_sydle,
    data_vencimento,
    status_fatura,
    classificacao_fatura,
    valor_fatura,
    aging,
    CASE
        WHEN pdd_arrasto.id_cliente_air IS NOT NULL AND classificacao_pdd IS NULL THEN 'PDD ARRASTO'
        WHEN pdd_arrasto.id_cliente_air IS NULL AND classificacao_pdd IS NULL AND aging <= 0 THEN 'A VENCER'
        WHEN pdd_arrasto.id_cliente_air IS NULL AND classificacao_pdd IS NULL AND aging BETWEEN 1 AND 90 THEN 'VENCIDO DE 1 A 90'
    ELSE classificacao_pdd END AS classificacao_pdd,
    classificacao_car,
    forma_pagamento,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao
FROM query_final
LEFT JOIN pdd_arrasto ON query_final.id_cliente_air = pdd_arrasto.id_cliente_air

UNION

SELECT contrato_air,
mes_referencia,
status_contrato,
segmento,
id_cliente_air,
tipo_pessoa,
polo,
marca,
cidade,
UF,
Regiao,
regional,
sub_regional,
codigo_fatura_sydle,
data_vencimento,
status_fatura,
classificacao_fatura,
valor_fatura,
aging,
classificacao_pdd,
classificacao_car,
forma_pagamento,
data_extracao
FROM {{ this }}
WHERE mes_referencia <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
