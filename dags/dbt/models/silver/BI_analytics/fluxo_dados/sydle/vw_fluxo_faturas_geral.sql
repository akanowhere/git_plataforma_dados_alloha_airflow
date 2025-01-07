SELECT  
    'SYDLE' as fonte,
    CAST(contrato_air AS BIGINT) AS contrato_air,
    CAST(id_cliente_air AS BIGINT) AS id_cliente_air,
    CAST(codigo_fatura_sydle AS BIGINT) AS codigo_fatura_sydle,
    id_legado_fatura,
    status_fatura,
    classificacao,
    CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    CAST(data_vencimento AS DATE) AS data_vencimento,
    CAST(data_pagamento AS DATE) AS data_pagamento, 
    CAST(valor_sem_multa_juros AS DOUBLE) valor_sem_multa_juros,
    CAST(valor_pago AS DOUBLE) AS valor_pago,
    beneficiario,
    mes_referencia,
    marca,
    cast(current_date() as date) as data_atualizacao_fluxo
 FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_mailing
 WHERE data_vencimento >= '2023-01-01' 