SELECT  
    'SYDLE' AS fonte,
    t1.contrato_air ,
    t1.id_cliente_air ,
    t1.codigo_fatura_sydle,
    t1.id_legado_fatura,
    t1.status_fatura ,
    t1.classificacao ,
    t1.data_criacao,
    t1.data_vencimento,
    t1.data_pagamento,
    t1.valor_sem_multa_juros,
    t1.valor_pago,
    t1.beneficiario,
    t1.mes_referencia,
    t1.marca,
    t3.cidade,
    t3.estado,
    CAST(current_date() AS DATE) AS data_atualizacao_fluxo
FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_mailing t1
LEFT JOIN (
    SELECT id_contrato, MAX(id_endereco) AS id_endereco
    FROM {{ get_catalogo('gold') }}.base.dim_contrato_entrega 
    GROUP BY id_contrato
) AS t2 ON t2.id_contrato = t1.contrato_air
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco t3 
    ON t2.id_endereco = t3.id_endereco
WHERE 
    status_fatura in ('EMITIDA','EM EMISSÃO','AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO','AGUARDANDO BAIXA EM SISTEMA EXTERNO')


