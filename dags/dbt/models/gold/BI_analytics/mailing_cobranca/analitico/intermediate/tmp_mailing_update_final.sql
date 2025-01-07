{{
    config(
        materialized='ephemeral'
    )
}}
{% set dates = get_dates(['today'],None, 'mailing_inicial') %}
{% set exclude_columns = [
        'fatura_motivadora',
        'aging_atual',
        'fila_ouvidoria_contestacao_fatura',
        'data_saida',
        'data_motivo_saida',
        'data_atualizacao_motivo_saida',
        'saida_tipo',
        'motivo_saida',
        'data_pagamento',
        'tipo_pagamento',
        'valor_pago',
        'data_cancelamento_contrato',
        'motivo_cancelamento_contrato'
    ] %}
{% set columns = get_columns('mailing','tbl_analitico', exclude_columns) %}
SELECT 
    mi.fatura_motivadora,
    {{ columns }},
    DATEDIFF(DAY, data_vencimento_antigo, '{{ dates.today }}') aging_atual,
    CASE 
        WHEN cha.codigo_contrato IS NOT NULL 
            THEN 1
        ELSE 0 
    END fila_ouvidoria_contestacao_fatura,
    COALESCE(uam.data_saida, mi.data_saida) AS data_saida,
    COALESCE(uam.data_motivo_saida, mi.data_motivo_saida) AS data_motivo_saida,
    COALESCE(uam.data_atualizacao_motivo_saida, mi.data_atualizacao_motivo_saida) AS data_atualizacao_motivo_saida,
    COALESCE(uam.saida_tipo, mi.saida_tipo) AS saida_tipo,
    COALESCE(uam.motivo_saida, mi.motivo_saida) AS motivo_saida,
    COALESCE(uam.data_pagamento, mi.data_pagamento) AS data_pagamento,
    COALESCE(uam.tipo_pagamento, mi.tipo_pagamento) AS tipo_pagamento,
    COALESCE(uam.valor_pago, mi.valor_pago) AS valor_pago,
    COALESCE(uam.data_cancelamento_contrato, mi.data_cancelamento_contrato) AS data_cancelamento_contrato,
    COALESCE(uam.motivo_cancelamento_contrato, mi.motivo_cancelamento_contrato) AS motivo_cancelamento_contrato
FROM {{ ref('tmp_mailing_update') }} mi
LEFT JOIN (
SELECT DISTINCT codigo_contrato
FROM {{ ref('dim_chamado') }} 
WHERE data_conclusao is null 
    AND (nome_fila = 'FINANCEIRO - CONTESTAÇÃO DE FATURA' OR nome_fila LIKE 'Ouvidoria%')
) cha
    ON mi.id_contrato = cha.codigo_contrato 
    AND mi.status_mailing = 'ATIVO' 
    AND mi.data_saida IS NULL 
LEFT JOIN {{ ref('update_all_motivos' ) }} uam
    ON mi.fatura_motivadora = uam.fatura_motivadora 
