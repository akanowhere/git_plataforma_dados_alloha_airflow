{{ config(
    materialized='ephemeral'
) }}

{% set dates = get_dates(['last_month'],None,'tmp_uce') %}

--### ULTIMA CHANCE ###--
SELECT 
    id_contrato, 
    faixa_aging as faixa_mailing_anterior,
    1 AS ultima_chance
FROM {{ get_catalogo('gold') }}.mailing.tbl_analitico
WHERE
    data_referencia = '{{ dates.last_month }}'
    AND status_mailing = 'ATIVO'
    AND motivo_saida = 'NEGOCIAÇÃO'
    AND tipo_pagamento = ''
    AND NOT (faixa_aging = '1-30d')
