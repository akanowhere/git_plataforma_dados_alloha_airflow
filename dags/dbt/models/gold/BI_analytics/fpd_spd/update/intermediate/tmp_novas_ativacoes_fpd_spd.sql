{{ config(
    materialized='ephemeral'
) }}

WITH cte_ativacoes AS (
    SELECT 
        t1.marca AS marca,
        t1.fonte AS fonte,
        t1.id_contrato AS contrato,
        t1.CODIGO_CONTRATO_AIR AS codigo_contrato_air,
        COALESCE(t3.cpf, t3.cnpj) AS cpf_cnpj,
        t1.data_ativacao AS data_ativacao,
        t4.id_vendedor AS codigo_vendedor,
        t4.nome_vendedor AS vendedor,
        COALESCE(t4.equipe,t7.equipe) AS equipe,
        CASE 
            WHEN COALESCE(t4.canal_tratado,t7.equipe) = 'EQUIPE_MIGRACAO' THEN 'MIGRACAO' 
            ELSE COALESCE(t4.canal_tratado,t7.equipe) 
        END AS canal_tratado,
        t4.unidade AS unidade,
        t5.estado AS uf,
        t5.cidade AS cidade,
        t5.bairro AS bairro,
        t5.cep AS cep,
        t5.logradouro AS logradouro,
        t5.numero AS numero,
        t5.complemento AS complemento,
        t5.referencia AS referencia,
        CONCAT(t1.id_contrato, '_', t1.fonte) AS contrato_fonte,
        NULL AS macro_regional,
        replace(t6.regional,'R','T')  AS regional,
        replace(t6.`cluster`,'CLUSTER_R','')  AS sub_regional,
        NULL AS codigo_fatura_sydle,
        NULL AS codigo_fatura,
        NULL AS data_vencimento,
        NULL AS data_pagamento,
        NULL AS data_primeiro_pagamento,
        NULL AS status_fatura,
        NULL AS valor_fatura,
        NULL AS data_cancelamento_fatura,
        NULL AS mes_vencimento,
        NULL AS aging_inadim,
        NULL AS aging,
        NULL AS flag_fb,
        NULL AS flag_fpd,
        NULL AS flag_fpd30d,
        NULL AS flag_fb30d,
        NULL AS flag_fpd20d,
        NULL AS flag_fb20d,
        NULL AS flag_fpd10d,
        NULL AS flag_fb10d,
        NULL AS codigo_segunda_fatura,
        NULL AS codigo_segunda_fatura_sydle,
        NULL AS status_segunda_fatura,
        NULL AS data_vencimento_segunda_fatura,
        NULL AS data_pagamento_segunda_fatura,
        NULL AS valor_fatura_segunda_fatura,
        NULL AS data_cancelamento_segunda_fatura,
        NULL AS flag_spd30d,
        NULL AS flag_sb30d,
        NULL AS aging_inadim_segunda_fatura,
        NULL AS aging_segunda_fatura,
        NULL AS flag_never_paid,
        NULL AS flg_over30m6,
        date_sub(current_date(), 1) AS data_referencia,
        CONVERT_TIMEZONE(CURRENT_TIMEZONE(), 'America/Sao_Paulo', NOW()) AS data_atualizacao    

    FROM gold.base.fato_primeira_ativacao t1
    LEFT JOIN gold.base.dim_contrato t2 ON (t1.id_contrato = t2.id_contrato)
    LEFT JOIN gold.base.dim_cliente t3 ON (t2.id_cliente = t3.id_cliente AND t3.fonte = 'AIR')
    LEFT JOIN gold.venda.fato_venda t4 ON (t1.CODIGO_CONTRATO_AIR = t4.id_contrato_air)
    LEFT JOIN gold.base.dim_endereco t5 ON (t2.id_endereco_cobranca = t5.id_endereco AND t5.fonte = 'AIR')
    LEFT JOIN gold.base.dim_unidade t6 ON (t5.unidade = t6.sigla AND t6.fonte = 'AIR' AND t6.excluido = false)
    LEFT JOIN silver.stage_venda.vw_venda_air_geral t7 ON (t1.codigo_contrato_air = t7.id_contrato AND t7.natureza = 'VN_NOVO_CONTRATO')
)
SELECT *
FROM cte_ativacoes
