{{ 
    config(
        materialized='ephemeral'
    ) 
}}

{% set dates = get_dates(['today'], None, 'update_contratos') %}

WITH update_cancelamentos AS (
    SELECT 
        mi.fatura_motivadora,
        '{{ dates.today }}' AS data_saida,
        ca.data_cancelamento AS data_motivo_saida,
        co.data_alteracao AS data_atualizacao_motivo_saida,
        'CONTRATO' AS saida_tipo,
        'CANCELAMENTO' AS motivo_saida,
        ca.data_cancelamento AS data_cancelamento_contrato,
        CASE 
            WHEN ca.cancelamento_invol IS NULL THEN 'VOLUNTÁRIO'
            WHEN ca.cancelamento_invol = 'SISTEMICO' THEN 'INVOLUNTÁRIO'
            ELSE 'INVOLUNTÁRIO_ADICIONAL'
        END motivo_cancelamento_contrato
        FROM {{ ref('tmp_mailing_update') }}  mi
        LEFT JOIN  {{ ref('fato_cancelamento') }} ca
            ON mi.id_contrato = ca.CODIGO_CONTRATO_AIR
        INNER JOIN {{ ref('dim_contrato') }} co
            ON ca.id_contrato = co.id_contrato 
                AND ca.data_cancelamento = co.data_cancelamento 
                --AND mi.versao_contrato < co.VERSAO_CONTRATO_AIR
        WHERE 
            mi.data_saida IS NULL
            AND mi.status_mailing = 'ATIVO'
            AND co.data_alteracao < '{{ dates.today }}'
)
, update_troca_titularidade AS (
    SELECT
        mi.fatura_motivadora,
        '{{ dates.today }}' AS data_saida,
        CAST(NULL AS DATE) AS data_motivo_saida,
        CAST(co.data_alteracao AS DATE) AS data_atualizacao_motivo_saida,
        'CONTRATO' AS saida_tipo,
        'TROCA_TITULARIDADE' AS motivo_saida,
        CAST(NULL AS DATE) data_cancelamento_contrato,
        CAST('' AS STRING) AS motivo_cancelamento_contrato
        FROM {{ ref('tmp_mailing_update') }} mi
        LEFT JOIN {{ ref('dim_contrato') }} co 
            ON mi.id_contrato = co.id_contrato
        WHERE 
            mi.data_saida IS NULL
            AND mi.id_cliente <> co.id_cliente
            --AND mi.versao_contrato + 1 = co.VERSAO_CONTRATO_AIR
            AND co.data_alteracao < '{{ dates.today }}'
            AND mi.fatura_motivadora NOT IN (SELECT fatura_motivadora FROM update_cancelamentos)

)
, update_troca_segmento AS (
    SELECT 
        mi.fatura_motivadora,
        '{{ dates.today }}' AS data_saida,
        CAST(NULL AS DATE) AS data_motivo_saida,
        CAST(co.data_alteracao AS DATE) AS data_atualizacao_motivo_saida,
        'CONTRATO' AS saida_tipo,
        'TROCA_SEGMENTO' AS motivo_saida,
        CAST(NULL AS DATE) data_cancelamento_contrato,
        CAST('' AS STRING) AS motivo_cancelamento_contrato
        FROM {{ ref('tmp_mailing_update') }}  mi
        INNER JOIN {{ ref('dim_contrato') }} co
            ON mi.id_contrato = co.id_contrato 
            --AND mi.versao_contrato + 1 = co.VERSAO_CONTRATO_AIR 
            AND co.b2b = true
        WHERE 
            mi.data_saida IS NULL
            AND co.data_alteracao < '{{ dates.today }}'

)

, final_table AS (
    SELECT * FROM update_cancelamentos
    UNION ALL
    SELECT * FROM update_troca_titularidade
    UNION ALL
    SELECT * FROM update_troca_segmento
)
SELECT 
    fatura_motivadora,
    data_saida,
    data_motivo_saida,
    CAST(data_atualizacao_motivo_saida AS DATE) AS data_atualizacao_motivo_saida,
    saida_tipo,
    motivo_saida,
    CAST(NULL AS DATE) data_pagamento,
    CAST('' AS STRING) AS tipo_pagamento,
	0.0 AS valor_pago,
    data_cancelamento_contrato,
    motivo_cancelamento_contrato
FROM final_table


