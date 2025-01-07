{% set data_corte = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=35)).strftime('%Y-%m-%d') %}

WITH cte_ativacoes_completa AS (
    SELECT * , 
        CASE WHEN codigo_fatura IS NULL OR aging <=35 THEN 1 ELSE 0 END flg_atualizar_FB,
        CASE WHEN codigo_segunda_fatura IS NULL OR aging_segunda_fatura <=35 THEN 1 ELSE 0 END flg_atualizar_SB
    FROM {{ this }} 
    UNION ALL 
    SELECT t1.* , 
        1 AS flg_atualizar_FB, 
        1 AS flg_atualizar_SB
    FROM {{ ref('tmp_novas_ativacoes_fpd_spd') }} t1
    LEFT JOIN {{ this }} t2 ON (t1.codigo_contrato_air = t2.codigo_contrato_air) 
    WHERE t1.data_ativacao >= '2024-06-01' AND t2.codigo_contrato_air IS NULL
)
SELECT DISTINCT 
        t1.marca,
        t1.fonte,
        t1.contrato,
        t1.codigo_contrato_air,
        t1.cpf_cnpj,
        CAST(t1.data_ativacao AS DATE) data_ativacao,
        COALESCE(t1.codigo_vendedor,t6.codigo_vendedor) AS codigo_vendedor,
        COALESCE(t1.vendedor,t6.vendedor) AS vendedor,
        COALESCE(t1.equipe,t6.equipe)AS equipe,
        COALESCE(t1.canal_tratado,t6.canal_tratado) AS canal_tratado,
        t1.unidade,
        t1.uf,
        t1.cidade,
        t1.bairro,
        t1.cep,
        t1.logradouro,
        t1.numero,
        t1.complemento,
        t1.referencia,
        t1.contrato_fonte,
        t1.macro_regional,
        COALESCE(t6.regional,t1.regional) AS regional,
        replace(COALESCE(t6.sub_regional,t1.sub_regional),'T','') sub_regional,
        COALESCE(t2.codigo_fatura_sydle, t1.codigo_fatura_sydle) AS codigo_fatura_sydle,
        COALESCE(t2.codigo_fatura, t1.codigo_fatura) AS codigo_fatura,
        COALESCE(t2.data_vencimento, t1.data_vencimento) AS data_vencimento,
        COALESCE(t2.data_pagamento, t1.data_pagamento) AS data_pagamento,
        COALESCE(t2.data_primeiro_pagamento, t1.data_primeiro_pagamento) AS data_primeiro_pagamento,
        COALESCE(t2.status_fatura, t1.status_fatura) AS status_fatura,
        COALESCE(t2.valor_fatura, t1.valor_fatura) AS valor_fatura,
        COALESCE(t2.data_cancelamento_fatura, t1.data_cancelamento_fatura) AS data_cancelamento_fatura,
        COALESCE(t2.mes_vencimento, t1.mes_vencimento) AS mes_vencimento,
        COALESCE(t2.aging_inadim, t1.aging_inadim) AS aging_inadim,
        COALESCE(t2.aging, t1.aging) AS aging,
        COALESCE(t2.flag_fb, t1.flag_fb) AS flag_fb,
        COALESCE(t2.flag_fpd, t1.flag_fpd) AS flag_fpd,
        COALESCE(t2.flag_fpd30d, t1.flag_fpd30d) AS flag_fpd30d,
        COALESCE(t2.flag_fb30d, t1.flag_fb30d) AS flag_fb30d,
        COALESCE(t2.flag_fpd20d, t1.flag_fpd20d) AS flag_fpd20d,
        COALESCE(t2.flag_fb20d, t1.flag_fb20d) AS flag_fb20d,
        COALESCE(t2.flag_fpd10d, t1.flag_fpd10d) AS flag_fpd10d,
        COALESCE(t2.flag_fb10d, t1.flag_fb10d) AS flag_fb10d,
        COALESCE(t3.codigo_fatura, t1.codigo_segunda_fatura) AS codigo_segunda_fatura,
        COALESCE(t3.codigo_fatura_sydle, t1.codigo_segunda_fatura_sydle) AS codigo_segunda_fatura_sydle,
        COALESCE(t3.status_fatura, t1.status_segunda_fatura) AS status_segunda_fatura,
        COALESCE(t3.data_vencimento, t1.data_vencimento_segunda_fatura) AS data_vencimento_segunda_fatura,
        COALESCE(t3.data_pagamento, t1.data_pagamento_segunda_fatura) AS data_pagamento_segunda_fatura,
        COALESCE(t3.valor_fatura, t1.valor_fatura_segunda_fatura) AS valor_fatura_segunda_fatura,
        COALESCE(t3.data_cancelamento_fatura, t1.data_cancelamento_segunda_fatura) AS data_cancelamento_segunda_fatura,
        COALESCE(t3.flag_fpd30d, t1.flag_spd30d) AS flag_spd30d,
        COALESCE(t3.flag_fb30d, t1.flag_sb30d) AS flag_sb30d,
        COALESCE(t3.aging_inadim, t1.aging_inadim_segunda_fatura) AS aging_inadim_segunda_fatura,
        COALESCE(t3.aging, t1.aging_segunda_fatura) AS aging_segunda_fatura,
        CASE
            WHEN t7.min_data_pagamento IS NOT NULL AND (t5.data_cancelamento IS NULL OR t7.min_data_pagamento < t5.data_cancelamento) THEN false 
            WHEN t1.aging <= 30 THEN false
            WHEN t1.data_primeiro_pagamento IS NOT NULL THEN false
            WHEN t7.min_data_pagamento IS NULL THEN true            
            ELSE false 
        END AS flag_never_paid,
        CASE WHEN t1.data_ativacao < date_sub(current_date(), 180) and t4.max_aging_atraso > 30 THEN true ELSE false END AS flg_over30m6,
        date_sub(current_date(), 1) AS data_referencia,
        COALESCE(t3.data_atualizacao,t2.data_atualizacao,t1.data_atualizacao) AS data_atualizacao
        
FROM cte_ativacoes_completa t1
LEFT JOIN {{ ref('tmp_faturas_fpd_spd') }} t2 ON (t1.codigo_contrato_air = t2.codigo_contrato_air AND t2.parcela = 1 AND t1.flg_atualizar_FB = 1)
LEFT JOIN {{ ref('tmp_faturas_fpd_spd') }} t3 ON (t1.codigo_contrato_air = t3.codigo_contrato_air AND t3.parcela = 2 AND t1.flg_atualizar_SB = 1)
LEFT JOIN (SELECT codigo_contrato_air,max(aging_inadim) as max_aging_atraso 
				 FROM {{ ref('tmp_faturas_fpd_spd') }}  
				 WHERE UPPER(status_fatura) in ('EM ABERTO', 'EMITIDA', 'AGUARDANDO BAIXA EM SISTEMA EXTERNO', 'AGUARDANDO PAGAMENTO EM SISTEMA EXTERNO', 'VENCIDA')
				 GROUP BY codigo_contrato_air) t4 ON (t1.codigo_contrato_air = t4.codigo_contrato_air)
LEFT JOIN gold.base.dim_contrato t5 ON (t1.codigo_contrato_air = t5.id_contrato)
LEFT JOIN {{ ref('tmp_novas_ativacoes_fpd_spd') }} t6 ON (t1.codigo_contrato_air = t6.codigo_contrato_air)
LEFT JOIN (SELECT codigo_contrato_air,min(data_pagamento) as min_data_pagamento 
				 FROM {{ ref('tmp_faturas_fpd_spd') }} 
				 GROUP BY codigo_contrato_air) t7 ON (t1.codigo_contrato_air = t7.codigo_contrato_air)
WHERE NOT(t1.flg_atualizar_FB = 1 AND t1.codigo_fatura_sydle IS NOT NULL AND t2.codigo_fatura_sydle IS NULL)
