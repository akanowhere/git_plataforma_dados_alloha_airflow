{{ config(
    materialized='table',
    alias='tbl_churn_voluntario_analitica'
) }}
WITH meses AS (
  SELECT explode(sequence(to_date('2024-05-31'), (last_day(CURRENT_DATE)), interval 1 month)) as referencia
)

, cte_fidelizados AS (
    SELECT DISTINCT
        referencia,
        a.CONTRATO_CODIGO_AIR as codigo_contrato_air,
        max(a.data_vencimento_fidelidade) as data_vencimento_fidelidade
    FROM meses
    LEFT JOIN (
        SELECT DISTINCT
            dc.codigo_externo AS CONTRATO_CODIGO_AIR, 
            CAST(dcd.data_fim_fidelidade AS DATE) AS data_vencimento_fidelidade
        FROM gold.sydle.dim_contrato_sydle dc
        LEFT JOIN gold.sydle.dim_contrato_descontos dcd
            ON dc.id_contrato = dcd.id_contrato
        WHERE (produto_nome = 'INSTALAÇÃO FIBRA'  OR produto_nome = 'instalacao fibra')
            AND desconto_nome LIKE '%multa de fidelidade%'
    )a ON a.data_vencimento_fidelidade >= meses.referencia
    GROUP BY ALL
)

, base_basao_inativa AS (
  SELECT 
    meses.referencia, 
    b.codigo_contrato_air id_contrato,
    b.ticket_final,
    b.valor_soma_itens,
    CASE 
      WHEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES', '') LIKE '%MB%'
        THEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES|MBPS', '')
      WHEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES', '') LIKE '%KB%'
        THEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES|KBPS', '')/1000
      WHEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES', '') LIKE '%GB%'
        THEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES|GBPS', '')*1000
    END velocidade_internet,
    cast((b.ticket_final -  b.valor_soma_itens) as decimal(10,0)) AS valor_desposicionado
  FROM meses
  CROSS JOIN gold_dev.churn_voluntario.base_inicial cadastro
  LEFT JOIN (
      SELECT *
      FROM gold.relatorios.tbl_basao
      WHERE data_referencia = CAST(date_add(CURRENT_DATE,-1) AS DATE) 
  ) b
  ON cadastro.contrato_air = b.codigo_contrato_air
  WHERE last_day(cadastro.data_cancelamento) <= referencia
)

, base_basao_ativos AS (

  SELECT 
    meses.referencia, 
    b.codigo_contrato_air id_contrato,
    b.ticket_final,
    b.valor_soma_itens,
    CASE 
      WHEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES', '') LIKE '%MB%'
        THEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES|MBPS', '')
      WHEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES', '') LIKE '%KB%'
        THEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES|KBPS', '')/1000
      WHEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES', '') LIKE '%GB%'
        THEN regexp_replace(UPPER(velocidade_internet), 'UP|RESIDENCIAL|DEDICADO|COMERCIAL|PME|RES|GBPS', '')*1000
    END velocidade_internet,
    cast((b.ticket_final -  b.valor_soma_itens) as decimal(10,0)) AS valor_desposicionado
  FROM meses
  LEFT JOIN (
    SELECT * 
    FROM gold.relatorios.tbl_basao
    WHERE data_referencia IN (SELECT referencia FROM meses)
  ) b
    ON last_day(b.data_referencia) = meses.referencia
  WHERE meses.referencia >= '2024-05-01'
)


, base_final AS (
    SELECT
        meses.referencia as data_referencia,
        cadastro.contrato_air,
        cadastro.regiao,    
        cadastro.regional,    
        cadastro.cluster,    
        cadastro.classificacao_guerra,    
        cadastro.tipo_cancelamento,    
        cadastro.data_cancelamento,
        cadastro.ano_mes_cancelamento,
        cadastro.ativacao_contrato as data_ativacao,  
        CASE
            WHEN TIMESTAMPDIFF(MONTH, cadastro.ativacao_contrato, meses.referencia) between 0 and 3 THEN 'Até 3 meses'
            WHEN TIMESTAMPDIFF(MONTH, cadastro.ativacao_contrato, meses.referencia) between 4 and 6 THEN 'De 4 a 6 meses'
            WHEN TIMESTAMPDIFF(MONTH, cadastro.ativacao_contrato, meses.referencia) between 7 and 12 THEN 'De 7 a 12 meses'
            WHEN TIMESTAMPDIFF(MONTH, cadastro.ativacao_contrato, meses.referencia) between 13 and 24 THEN 'De 13 a 24 meses'
            WHEN TIMESTAMPDIFF(MONTH, cadastro.ativacao_contrato, meses.referencia) > 24 THEN 'Acima de 24 meses'
            ELSE 'Não ativado'
        END AS safra_de_ativacao,
        sac_suporte.qtd_chamadas_total_st,
        case when sac_suporte.qtd_chamadas_total_st >= 1 then 1 else 0 end as flag_chamadas_st_60d,
        case when sac_suporte.qtd_chamadas_total_st > 1 then 1 else 0 end as flag_reincidente_chamadas_st_60d,
        sac_suporte.qtd_chamadas_total_sac,
        case when sac_suporte.qtd_chamadas_total_sac >= 1 then 1 else 0 end as flag_chamados_sac_60d,
        case when sac_suporte.qtd_chamadas_total_sac > 1 then 1 else 0 end as flag_reincidente_chamadas_sac_60d,
        mudende_reparo.qtd_mudend_60d,
        mudende_reparo.qtd_reparo_60d,
        case when mudende_reparo.qtd_reparo_60d >= 1 then 1 else 0 end as flag_reparo_60d,
        case when mudende_reparo.qtd_reparo_60d > 1 then 1 else 0 end as flag_reincidente_reparo_60d,
        reajuste_susp.qtd_suspenso_60d,
        case when COALESCE(reajuste_susp.qtd_suspenso_60d,0) >= 1 then 1 else 0 end as flag_suspenso_60d,
        chamado_financeiro.qtd_chamados_financeiros_60d,
        case when chamado_financeiro.qtd_chamados_financeiros_60d >= 1 then 1 else 0 end as flag_chamados_financeiros_60d,
        case when chamado_financeiro.qtd_chamados_financeiros_60d > 1 then 1 else 0 end as flag_reincidente_chamados_financeiros_60d,
        basao.ticket_final,
        case 
            when basao.ticket_final <= 59.99 then 'Até R$59'
            when basao.ticket_final <= 79.99 then 'De R$60 a R$79'
            when basao.ticket_final <= 89.99 then 'De R$80 a R$89'
            when basao.ticket_final <= 99.99 then 'De R$90 a R$99'
            when basao.ticket_final <= 109.99 then 'De R$100 a R$109'
            when basao.ticket_final > 109.99 then 'Acima de R$109'
            else 'De R$90 a R$99' 
        end as faixa_fatura_bruta,
        fidelidade.data_vencimento_fidelidade,
        CASE 
            WHEN fidelidade.data_vencimento_fidelidade IS NULL THEN 'Sem Fidelização'
            WHEN TIMESTAMPDIFF(month,now(),fidelidade.data_vencimento_fidelidade) BETWEEN 0 AND 3 THEN 'Fidelizado até 3 meses'
            WHEN TIMESTAMPDIFF(month,now(),fidelidade.data_vencimento_fidelidade) BETWEEN 4 AND 6 THEN 'Fidelizado de 4 a 6 meses'
            WHEN TIMESTAMPDIFF(month,now(),fidelidade.data_vencimento_fidelidade) > 6 THEN 'Fidelizado > 6 meses'
            ELSE 'Sem Fidelização' 
        END AS fidelizacao,
        ult_pag.data_ultimo_pagamento,
        case 
            when datediff(meses.referencia,ult_pag.data_ultimo_pagamento) <= 30 then 'Até 30 dias'
            WHEN datediff(meses.referencia,ult_pag.data_ultimo_pagamento) <= 60 then 'De 31 a 60 dias'
            WHEN datediff(meses.referencia,ult_pag.data_ultimo_pagamento) <= 90 then 'De 61 a 90 dias'
            WHEN datediff(meses.referencia,ult_pag.data_ultimo_pagamento) <= 120 then 'De 91 a 120 dias'
            WHEN datediff(meses.referencia,ult_pag.data_ultimo_pagamento) <= 150 then 'De 120 a 150 dias'
            WHEN datediff(meses.referencia,ult_pag.data_ultimo_pagamento) > 150 then 'Acima de 150 dias'
            else 'Sem Pagamento'
        end as faixa_ultimo_pagamento,
        case 
            when (cadastro.data_cancelamento is null 
                or cast(cadastro.data_cancelamento as date) > meses.referencia)
                and cast(cadastro.ativacao_contrato as date) <= meses.referencia 
            then 1
            else 0
        end as flag_base_ativa,
        SUM(
            IF(sac_suporte.qtd_chamadas_total_st >= 1, 1, 0) +
            IF(sac_suporte.qtd_chamadas_total_sac >= 1, 1, 0) +
            IF(mudende_reparo.qtd_mudend_60d >= 1, 1, 0) +
            IF(mudende_reparo.qtd_reparo_60d >= 1, 1, 0) +
            IF(chamado_financeiro.qtd_chamados_financeiros_60d >= 1, 1, 0)
    ) AS flags_totais,
    CASE WHEN tbl_onu.contrato_air IS NOT NULL THEN 1 ELSE 0 END flag_onu,
    CASE WHEN tbl_monitoramento.id_cliente IS NOT NULL THEN 1 ELSE 0 END flag_monitoramento_rede,
    case 
        when basao.valor_desposicionado between 5 and 10 then '5R$ até 10R$'
        when basao.valor_desposicionado between 11 and 15 then '11R$ até 15R$'
        when basao.valor_desposicionado between 16 and 20 then '16R$ até 20R$'
        when basao.valor_desposicionado  >= 21 then '21R$ >' 
        ELSE '< 5R$'
        end as faixa_desposicionado,
    case 
        when basao.valor_desposicionado >= 5 OR basao.velocidade_internet < 300 then 1 
        else 0 
    end as flag_desposicionado
    FROM meses
    CROSS JOIN gold_dev.churn_voluntario.base_inicial cadastro
    LEFT JOIN gold_dev.churn_voluntario.tbl_sac_suporte sac_suporte
        ON sac_suporte.referencia = meses.referencia AND sac_suporte.contrato_air =  cadastro.contrato_air
    LEFT JOIN gold_dev.churn_voluntario.tbl_mudende_reparo mudende_reparo
        ON mudende_reparo.referencia = meses.referencia AND mudende_reparo.contrato_air =  cadastro.contrato_air
    LEFT JOIN gold_dev.churn_voluntario.tbl_chamado_financeiro chamado_financeiro
        ON chamado_financeiro.referencia = meses.referencia AND chamado_financeiro.contrato_air =  cadastro.contrato_air
    LEFT JOIN gold_dev.churn_voluntario.tbl_ult_fat ult_fat
        ON ult_fat.referencia = meses.referencia AND ult_fat.contrato_air =  cadastro.contrato_air
    LEFT JOIN cte_fidelizados fidelidade
        ON fidelidade.referencia = meses.referencia AND fidelidade.codigo_contrato_air = cadastro.contrato_air 
    LEFT JOIN gold_dev.churn_voluntario.tbl_reajuste_susp reajuste_susp
        ON reajuste_susp.referencia = meses.referencia AND reajuste_susp.contrato_air = cadastro.contrato_air 
    LEFT JOIN gold_dev.churn_voluntario.tbl_ult_pag ult_pag
        ON ult_pag.referencia = meses.referencia AND ult_pag.contrato_air = cadastro.contrato_air 
    LEFT JOIN gold_dev.churn_voluntario.tbl_onu_cliente_280k tbl_onu  
        ON tbl_onu.contrato_air = cadastro.contrato_air 
    LEFT JOIN gold.base.dim_outros_marcadores tbl_monitoramento
        ON tbl_monitoramento.id_cliente = cadastro.id_cliente_air AND UPPER(marcador) LIKE '%ELEG_MONIT_HUAWEI%'
    LEFT JOIN (
        SELECT *
        FROM base_basao_ativos
        UNION 
        SELECT *
        FROM base_basao_inativa
    ) basao
        ON basao.referencia = meses.referencia AND basao.id_contrato = cadastro.contrato_air 
    GROUP BY ALL
)


SELECT  
    *,
    CASE        
        WHEN flags_totais = 0            
            and safra_de_ativacao in (
                'Acima de 24 meses',            
                'De 13 a 24 meses',            
                'De 7 a 12 meses'
            )            
            and regiao in ('SUDESTE 2','SUDESTE 1')            
            and faixa_fatura_bruta = 'Até R$59' 
        THEN  '1.1'        
        WHEN flags_totais = 0            
            and safra_de_ativacao in ('Acima de 24 meses','De 13 a 24 meses')            
            and regiao in ('SUDESTE 2','SUDESTE 1')            
            and faixa_fatura_bruta = 'De R$60 a R$79'            
            and classificacao_guerra = 'GUERRA TOTAL' THEN '1.2'        
        WHEN flags_totais = 0            
            and safra_de_ativacao in (
                'Acima de 24 meses',            
                'De 13 a 24 meses',            
                'De 7 a 12 meses'
            )            
            and regiao in ('NORDESTE')            
            and faixa_fatura_bruta = 'Até R$59' 
        THEN '1.3'        
        WHEN flags_totais = 0            
            and regiao in ('NORDESTE')            
            and faixa_fatura_bruta = 'Acima de R$109' 
        THEN '1.4'        
        WHEN flags_totais = 0 
        THEN '1.5'        
        WHEN flags_totais = 1 THEN '2.1'        
        WHEN flags_totais = 2 THEN '2.2'        
        WHEN flags_totais >= 3 THEN '2.3'
    END AS subcluster_1_2,
    CASE
        -- WHEN fidelizacao = 'Sem Fidelização' THEN '3.1.1'
        -- WHEN fidelizacao <> 'Sem Fidelização' THEN '3.1.2'        
        WHEN Fidelizacao = 'Sem Fidelização' AND flag_desposicionado = 0 THEN '3.1.1.1'        
        WHEN Fidelizacao = 'Sem Fidelização' AND flag_desposicionado = 1 THEN '3.1.1.2'        
        WHEN Fidelizacao <> 'Sem Fidelização' AND flag_desposicionado = 0 THEN '3.1.2.1'        
        WHEN Fidelizacao <> 'Sem Fidelização' AND flag_desposicionado = 1 THEN '3.1.2.2'   
    END AS subcluster_3_1,
    CASE      
        WHEN flag_reincidente_chamados_financeiros_60d = 1 THEN '3.2.1'        
        WHEN flag_reincidente_chamadas_st_60d = 1 THEN '3.2.2'        
        WHEN flag_reincidente_chamadas_sac_60d = 1 THEN '3.2.3'        
        WHEN flag_reincidente_reparo_60d = 1 THEN '3.2.4'
    END subcluster_3_2,        
    CASE      
        WHEN flag_onu = 1 THEN '3.3.1'   
    END subcluster_3_3,       
    CASE      
        WHEN flag_monitoramento_rede = 1 THEN '3.4.1'   
    END subcluster_3_4,        
        -- WHEN flag_chamadas_st_60d >= 1 THEN '3.4.1'        
        -- WHEN flag_reparo_60d >= 1 THEN '3.4.2'        
    CASE      
        WHEN flag_suspenso_60d = 1            
            and faixa_ultimo_pagamento = 'De 31 a 60 dias' 
        THEN '3.5.1'    
    END subcluster_3_5        
FROM base_final