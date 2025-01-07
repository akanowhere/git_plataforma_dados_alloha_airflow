-- Query Relatorio ContratosParaSuspensao
-- Obs: ajustar tabela contrato_entrega quando tiver pronta
-- Faturas
{% set dates = get_dates(['yesterday', 'today']) %}

WITH cte_faturas AS (
    SELECT 
        codigo_fatura_sydle,
        codigo_contrato_air AS contrato_air,
        codigo_cliente_air AS id_cliente_air,
        status_fatura,
        classificacao,
        data_criacao,
        data_vencimento,
        mes_referencia,
        valor_sem_multa_juros,
        DATEDIFF(DAY, data_vencimento, '{{ dates.yesterday }}') AS aging
    FROM {{ get_catalogo('gold') }}.sydle.dim_faturas_all_versions
    WHERE is_last = true 
    AND data_vencimento < DATEADD(DAY, -11, '{{ dates.today }}')
      AND data_pagamento IS NULL
      AND status_fatura IN (
        'AGUARDANDO BAIXA EM SISTEMA EXTERNO', 
        'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO', 
        'EMITIDA'
        )
),
-- Ultimo Endereço
cte_ultimo_endereco AS (
	SELECT 
        id_contrato, 
        MAX(id_endereco) AS id_endereco
    FROM {{ get_catalogo('gold') }}.base.dim_contrato_entrega
    WHERE excluido = true
    GROUP BY id_contrato
)
SELECT
    DISTINCT
    t6.marca AS marca,
    t1.contrato_air AS codigo_contrato_air,
    t1.id_cliente_air AS codigo_cliente,
    REPLACE(t2.status,'ST_CONT_','') AS status_contrato,
    COALESCE(t3.cpf, t3.cnpj) AS cpf_cnpj,
    t3.tipo AS tipo_pessoa,
    t1.codigo_fatura_sydle AS codigo_fatura_sydle,
    t1.status_fatura AS status_fatura,
    t1.classificacao AS classificacao,
    t1.data_criacao AS data_criacao,
    t1.data_vencimento AS data_vencimento,
    t1.mes_referencia AS mes_referencia,
    t1.aging AS aging,
    t1.valor_sem_multa_juros AS valor_original,
    t5.cidade AS cidade,
    t5.estado AS estado,
    t2.nome_suspensao AS regra_suspensao, 
    t2.dias_atraso_suspensao AS atraso_para_suspensao,
    '{{ dates.yesterday }}' AS data_referencia
FROM cte_faturas t1
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato t2 
    ON t1.contrato_air = t2.id_contrato
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente t3 
    ON t1.id_cliente_air = t3.id_cliente AND t3.fonte = 'AIR'
LEFT JOIN cte_ultimo_endereco t4 
    ON t1.contrato_air = t4.id_contrato
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco t5 
    ON t4.id_endereco = t5.id_endereco AND t5.fonte = 'AIR'
LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade t6 
    ON t2.unidade_atendimento = t6.sigla AND t6.fonte = 'AIR'
WHERE t2.status <> 'ST_CONT_CANCELADO'