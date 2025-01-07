WITH vw_faturas_mailing AS (
    SELECT *
    FROM {{ ref('dim_faturas_all_versions') }} AS dim_faturas_all_versions
    WHERE is_last = TRUE
),

fatura_negociadas AS (
    SELECT t1.codigo_fatura_sydle,
           t1.codigo_contrato_air,
           t1.codigo_cliente_air,
           t1.valor_sem_multa_juros,
           t1.status_fatura,
           t1.data_vencimento,
           t1.classificacao,
           t1.data_pagamento,
           t1.valor_pago
    FROM vw_faturas_mailing t1
    WHERE DATE(t1.data_atualizacao) >= '2023-01-01'
)

SELECT
    A.id_negociacao,
    B.codigo_contrato_air AS contrato_air,
    B.codigo_cliente_air as id_cliente_air,
    CASE
        WHEN dim_contrato.b2b = TRUE THEN 'B2B'
        WHEN dim_contrato.pme = TRUE THEN 'PME'
        ELSE 'B2C'
    END AS segmento,
    endereco_contrato.cidade,
    endereco_contrato.estado AS uf,
    U.marca,
    U.regional,
    A.codigo AS fatura_gerada,
    B.valor_sem_multa_juros AS valor_fatura_gerada,
    B.status_fatura AS status_fatura_gerada,
    B.data_vencimento AS data_vencimento_fatura_gerada,
    B.data_pagamento AS data_pgt_fatura_gerada,
    B.valor_pago AS vlr_pgt_fatura_gerada,
    A.fatura_original,
    C.valor_sem_multa_juros AS valor_fatura_original,
    C.data_vencimento AS data_vencimento_fatura_original,
    C.classificacao AS classificacao_fatura_original,
    C.data_pagamento AS data_pgt_fatura_original,
    C.valor_pago AS vlr_pgt_fatura_original,
    A.id_negociador,
    A.nm_negociador AS nome_negociador,
    A.data_negociacao,
    CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS dt_atualizacao

FROM {{ ref('dim_negociacao') }} AS A
INNER JOIN fatura_negociadas B ON A.codigo = B.codigo_fatura_sydle
INNER JOIN fatura_negociadas C ON A.fatura_original = C.codigo_fatura_sydle
LEFT JOIN (
    SELECT E.estado,
           E.cidade,
           E.unidade,
           A.id_contrato_air AS CODIGO_CONTRATO_AIR
    FROM {{ ref('dim_contrato') }} AS A
    INNER JOIN (
        SELECT MAX(id_endereco) AS endereco,
               id_contrato
        FROM {{ ref('dim_contrato_entrega') }} AS dim_contrato_entrega
        GROUP BY id_contrato
    ) AS tb_temp ON tb_temp.id_contrato = A.id_contrato_air
    INNER JOIN {{ ref('dim_endereco') }} AS E ON E.id_endereco = tb_temp.endereco
) AS endereco_contrato ON endereco_contrato.CODIGO_CONTRATO_AIR = B.codigo_contrato_air
LEFT JOIN {{ ref('dim_contrato') }} AS dim_contrato ON B.codigo_contrato_air = dim_contrato.id_contrato_air
LEFT JOIN {{ ref('dim_unidade') }} AS U ON U.sigla = COALESCE(endereco_contrato.unidade, dim_contrato.unidade_atendimento)
AND UPPER(U.fonte) = 'AIR' AND U.excluido = FALSE
WHERE DATE(A.data_negociacao) >= '2023-01-01'
