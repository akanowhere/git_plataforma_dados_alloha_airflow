{{ 
    config(
        materialized='ephemeral'
    ) 
}}

{% set dates = get_dates(['today']) %}


WITH tmp_contestacao_ouvidoria AS (
    SELECT 
        cha.codigo_contrato
    FROM {{ ref('tmp_mailing_inicial') }} mi
    LEFT JOIN {{ ref('dim_chamado') }} cha
        ON mi.id_contrato = cha.codigo_contrato 
            AND mi.status_mailing = 'ATIVO' 
            AND mi.data_saida IS NULL 
    WHERE 
        data_conclusao is null 
        AND (nome_fila = 'FINANCEIRO - CONTESTAÇÃO DE FATURA' OR nome_fila LIKE 'Ouvidoria%')
)

SELECT
    mi.fatura_motivadora,
    CASE 
        WHEN co.codigo_contrato IS NOT NULL 
            THEN 1
        ELSE 0 
    END fila_ouvidoria_contestacao_fatura
FROM {{ ref('tmp_mailing_inicial') }} mi
LEFT JOIN tmp_contestacao_ouvidoria co
    ON mi.id_contrato = co.codigo_contrato