{{
    config(
        materialized='ephemeral'
    )
}}
{% set dates = get_dates(['first_day_month', 'today'],None, 'mailing_inicial') %}
{% set exclude_columns = [
        'fatura_motivadora',
        'aging_atual',
        'fila_ouvidoria_contestacao_fatura',
    ] %}
{% set columns = get_columns('mailing','tbl_analitico', exclude_columns) %}	
SELECT 
    DISTINCT
    mi.fatura_motivadora,
    {{ columns }},
     DATEDIFF(DAY, data_vencimento_antigo, '{{ dates.today }}') aging_atual,
    CASE 
        WHEN cha.codigo_contrato IS NOT NULL 
            THEN 1
        ELSE 0 
    END fila_ouvidoria_contestacao_fatura
FROM {{ ref('tmp_mailing_inicial' ) }} mi
LEFT JOIN (
    SELECT DISTINCT codigo_contrato
    FROM {{ ref('dim_chamado') }} 
    WHERE data_conclusao is null 
        AND (nome_fila = 'FINANCEIRO - CONTESTAÇÃO DE FATURA' OR nome_fila LIKE 'Ouvidoria%')
) cha
    ON mi.id_contrato = cha.codigo_contrato 
    AND mi.status_mailing = 'ATIVO' 
    AND mi.data_saida IS NULL