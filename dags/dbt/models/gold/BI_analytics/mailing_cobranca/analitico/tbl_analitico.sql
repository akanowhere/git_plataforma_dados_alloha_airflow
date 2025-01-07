{{ 
    config(
        materialized='incremental',
        unique_key=['fatura_motivadora',  'data_referencia']
    )
}}

{% set dates = get_dates(['first_day_month', 'today'],None,'tbl_analitico') %}

SELECT 
    DISTINCT 
    fatura_motivadora,
    marca,
    id_contrato,
    versao_contrato,
    status_contrato,
    nome_cliente,
    id_cliente,
    cpf_cnpj,
    tipo_documento,
    versao_fatura,
    status_fatura,
    mes_vencimento,
    data_vencimento_antigo,
    data_ultimo_pagamento,
    negociacao,
    aging_mes,
    divida_atual,
    faturas_abertas,
    cidade,
    regional,
    faixa_aging,
    status_mailing,
    localidade,
    ultima_chance,
    faixa_mailing_anterior,
    valor_fatura_motivadora,
    data_referencia,
    aging_atual,
    fila_ouvidoria_contestacao_fatura,
    data_saida,
    data_motivo_saida,
    data_atualizacao_motivo_saida,
    saida_tipo,
    motivo_saida,
    data_pagamento,
    tipo_pagamento,
    valor_pago,
    data_cancelamento_contrato,
    motivo_cancelamento_contrato
FROM {{ ref('tmp_mailing_update_final' ) }}

{% if dates.today.day == 1 %}
UNION
    SELECT
        DISTINCT
        fatura_motivadora,
        marca,
        id_contrato,
        versao_contrato,
        status_contrato,
        nome_cliente,
        id_cliente,
        cpf_cnpj,
        tipo_documento,
        versao_fatura,
        status_fatura,
        mes_vencimento,
        data_vencimento_antigo,
        data_ultimo_pagamento,
        negociacao,
        aging_mes,
        divida_atual,
        faturas_abertas,
        cidade,
        regional,
        faixa_aging,
        status_mailing,
        localidade,
        ultima_chance,
        faixa_mailing_anterior,
        valor_fatura_motivadora,
        data_referencia,
        aging_atual,
        fila_ouvidoria_contestacao_fatura,
        data_saida,
        data_motivo_saida,
        data_atualizacao_motivo_saida,
        saida_tipo,
        motivo_saida,
        data_pagamento,
        tipo_pagamento,
        valor_pago,
        data_cancelamento_contrato,
        motivo_cancelamento_contrato
    FROM {{ ref('tmp_mailing_inicial_final' ) }}
{% endif %}