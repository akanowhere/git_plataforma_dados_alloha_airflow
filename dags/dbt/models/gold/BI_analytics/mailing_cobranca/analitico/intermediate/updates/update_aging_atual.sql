{{ 
    config(
        materialized='ephemeral'
    ) 
}}

{% set dates = get_dates(['today']) %}

SELECT 
    fatura_motivadora,
    DATEDIFF(DAY, data_vencimento_antigo, '{{ dates.today }}') aging_atual
FROM {{ ref('tmp_mailing_inicial') }}
