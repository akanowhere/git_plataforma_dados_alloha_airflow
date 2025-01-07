-- Query contratos Suspensos
-- Ultimo endereço

{% set dates = get_dates(['yesterday', 'today']) %}

WITH CTE_Endereco AS (
    SELECT 
        id_contrato, 
        MAX(id_endereco) AS id_endereco
    FROM {{ get_catalogo('gold') }}.base.dim_contrato_entrega
    WHERE excluido = true
    GROUP BY id_contrato
)
--ultima suspensao por debito
, CTE_ClienteEvento AS (
    SELECT 
        t1.id_contrato, 
        t1.momento AS data_suspensao,
        TRY_CAST(SUBSTRING(t1.observacao, INSTR(t1.observacao, 'suspensão:') + 10, 9) AS INT) AS codigo_fatura_sydle
    FROM {{ get_catalogo('gold') }}.base.dim_cliente_evento t1
    INNER JOIN (
        SELECT 
            id_contrato, 
            MAX(momento) AS momento_maximo
        FROM {{ get_catalogo('gold') }}.base.dim_cliente_evento
        WHERE tipo = 'EVT_CONTRATO_SUSPENSO' AND observacao LIKE '%débito%'
        GROUP BY id_contrato
    ) t2 ON t1.id_contrato = t2.id_contrato AND t1.momento = t2.momento_maximo
)
, CTE_ClienteEventoSuspensao AS (
    SELECT 
        t1.id_contrato, 
        t1.momento AS data_suspensao,
        TRY_CAST(SUBSTRING(t1.observacao, INSTR(t1.observacao, 'suspensão:') + 10, 9) AS INT) AS codigo_fatura_sydle
    FROM {{ get_catalogo('gold') }}.base.dim_cliente_evento t1
    INNER JOIN (
        SELECT 
            id_contrato, 
            MAX(momento) AS momento_maximo
        FROM {{ get_catalogo('gold') }}.base.dim_cliente_evento
        WHERE tipo = 'EVT_CONTRATO_SUSPENSO'
        GROUP BY id_contrato
    ) t2 ON t1.id_contrato = t2.id_contrato AND t1.momento = t2.momento_maximo
)
, CTE_Contratos AS (
    SELECT 
        t1.id_contrato AS contrato_air,
        t1.id_cliente AS codigo_cliente,
        t1.status AS status_contrato,
        COALESCE(t2.cpf, t2.cnpj) AS cpf_cnpj,
        t2.tipo AS tipo_pessoa,
        t4.cidade,
        t4.estado,
        t7.marca,
        COALESCE(t5.data_suspensao, t8.data_suspensao) AS data_suspensao,
        t5.codigo_fatura_sydle,
        t6.saldo_habilitacao_confianca_valor_saldo_atual
    FROM {{ get_catalogo('gold') }}.base.dim_contrato t1
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente t2 
        ON t1.id_cliente = t2.id_cliente AND t2.fonte = 'AIR'
    LEFT JOIN CTE_Endereco t3 
        ON t1.id_contrato = t3.id_contrato
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco t4 
        ON t3.id_endereco = t4.id_endereco AND t4.fonte = 'AIR'
    LEFT JOIN CTE_ClienteEvento t5 
        ON t1.id_contrato = t5.id_contrato
    LEFT JOIN {{ get_catalogo('gold') }}.sydle.dim_contrato_sydle t6 
        ON t1.id_contrato = t6.codigo_externo AND t6.status_status_servico = 'Suspenso por inadimplência'
    LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade t7 
        ON t1.unidade_atendimento = t7.sigla AND t7.fonte = 'AIR' AND t7.excluido = true
    LEFT JOIN CTE_ClienteEventoSuspensao t8 
        ON t1.id_contrato = t8.id_contrato
    WHERE t1.status = 'ST_CONT_SUSP_DEBITO'
)
SELECT DISTINCT
    t1.data_suspensao AS data_suspensao,
    t1.saldo_habilitacao_confianca_valor_saldo_atual AS saldo_habilitacao,
    t1.marca AS marca,
    t1.contrato_air AS codigo_contrato_air,
    t1.codigo_cliente AS codigo_cliente,
    t1.status_contrato AS status_contrato,
    t1.cpf_cnpj AS cpf_cnpj,
    t1.tipo_pessoa AS tipo_pessoa,
    t1.codigo_fatura_sydle AS codigo_fatura_sydle,
    t2.status_fatura AS status_fatura,
    t2.classificacao AS classificacao,
    t2.data_criacao AS data_criacao,
    t2.data_vencimento AS data_vencimento,
    t2.data_pagamento AS data_pagamento,
    t2.mes_referencia AS mes_referencia,
    DATEDIFF('{{ dates.today }}', date_sub(t2.data_vencimento, 1)) AS aging,
    t2.valor_sem_multa_juros AS valor_original,
    t2.valor_pago AS valor_pago,
    t1.cidade AS cidade,
    t1.estado AS estado,
    '{{ dates.yesterday }}' as data_referencia
FROM CTE_Contratos t1
LEFT JOIN {{ get_catalogo('gold') }}.sydle.dim_faturas_all_versions t2 
    ON t1.codigo_fatura_sydle = t2.codigo_fatura_sydle AND t2.is_last = TRUE  