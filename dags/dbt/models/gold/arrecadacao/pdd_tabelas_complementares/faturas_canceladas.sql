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

dados_faturas_canceladas AS (
    SELECT DISTINCT A.contrato_air,
    CASE
        WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6 
        THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
        ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
    END AS mes_referencia,
    A.codigo_fatura_sydle,
    A.data_atualizacao,
    A.data_criacao,
    A.data_vencimento,
    A.status_fatura,
    A.classificacao AS classificacao_fatura,
    A.valor_fatura

    FROM {{ ref('dim_faturas_mailing') }} A

    where YEAR(A.data_atualizacao) = YEAR(DATEADD(DAY, -1, DATEADD(HOUR, -3, current_date())))
    AND MONTH(A.data_atualizacao) = MONTH(DATEADD(DAY, -1, DATEADD(HOUR, -3, current_date())))
    AND UPPER(A.status_fatura) = 'CANCELADA'
)

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
A.data_atualizacao,
A.data_criacao,
A.data_vencimento,
A.status_fatura,
A.classificacao_fatura,
A.valor_fatura,
CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_extracao

FROM dados_faturas_canceladas A
LEFT JOIN dados_contrato B
    ON A.contrato_air = B.id_contrato_air
LEFT JOIN dados_endereco C
    ON A.contrato_air = C.id_contrato_air

union

SELECT DISTINCT contrato_air,
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
data_atualizacao,
data_criacao,
data_vencimento,
status_fatura,
classificacao_fatura,
valor_fatura,
data_extracao
from {{ this }}
where mes_referencia <>
CASE
    WHEN LENGTH(CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))) = 6
    THEN CONCAT('0', CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date()))))
    ELSE CONCAT(MONTH(DATEADD(DAY, -1, current_date())), '/', YEAR(DATEADD(DAY, -1, current_date())))
END
