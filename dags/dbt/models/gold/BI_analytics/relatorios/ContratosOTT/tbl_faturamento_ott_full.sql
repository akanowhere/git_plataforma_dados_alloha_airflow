WITH produtos AS (
    SELECT  DISTINCT
            t1.nome as parceiro,
            COALESCE(t5.codigo,t4.codigo,t2.urn) as produto_id,
            t3.codigo as codigo_produto,
            t2.urn AS sku,
            t2.descricao AS produto     
    FROM gold.tv.dim_sva t1
    INNER JOIN gold.tv.dim_sva_urn t2 ON (t1.id = t2.id_sva)
    INNER JOIN gold.tv.dim_sva_produto_urn t3 ON (t2.id = t3.id_urn)
    LEFT JOIN gold.tv.dim_produtos_watch t4 ON (UPPER(t2.urn) = UPPER(t4.sku))
    LEFT JOIN gold.tv.dim_produtos_gigamaistv t5 ON (UPPER(t2.urn) = UPPER(t5.sku))
    WHERE t1.nome in ('GLOBOPLAY','WATCH','WATCH_GIGA+FIBRA')  
)
,contratos AS (
    SELECT DISTINCT p.parceiro,
           p.codigo_produto,
           p.produto_id,
           p.sku,
           p.produto,
           dcp.id_contrato,
           dc.id_cliente as codigo_cliente,
           dcp.data_criacao,
           dcp.versao_contrato,
           TRIM(UPPER(dcp.item_nome)) AS produto_nome,
           dci.item_codigo,
           TRIM(UPPER(dci.item_nome)) AS item_nome,
           dci.valor_final AS valor_item,
           du.marca,
           e.unidade,
           e.cidade,
           e.estado,
           GREATEST(dc.data_primeira_ativacao,CAST(dcp.data_criacao AS DATE)) data_ativacao,
           LEAST(CAST(dm.data_venda AS DATE),dc.data_cancelamento,(CAST(CURRENT_DATE() AS DATE)-1)) AS data_fim,
           LEAST(CAST(dm.data_venda AS DATE),dc.data_cancelamento) AS data_fim_plano
    FROM gold.base.dim_contrato dc
    INNER JOIN gold.base.dim_contrato_produto dcp ON dc.id_contrato = dcp.id_contrato and dcp.excluido = false    
    INNER JOIN gold.base.dim_contrato_item dci ON dcp.id_contrato_produto = dci.id_contrato_produto AND dcp.id_contrato = dci.id_contrato AND dci.excluido = false
    INNER JOIN produtos p ON dcp.item_codigo = p.codigo_produto
    INNER JOIN gold.base.dim_endereco e ON e.id_endereco = dc.id_endereco_cobranca
    LEFT JOIN gold.base.dim_unidade du ON e.unidade = du.sigla
    LEFT JOIN gold.base.dim_migracao dm ON dcp.id_contrato = dm.id_contrato AND dcp.versao_contrato = dm.antigo_versao and dm.reprovada = false
    WHERE dc.data_primeira_ativacao IS NOT NULL
 
)
,fatura_itens AS (
    SELECT DISTINCT dfi.codigo_fatura_sydle,
        TRIM(UPPER(dfi.nome_item)) AS nome_item,
        ROUND(SUM(dfi.valor_item),2) as valor_final,       
        MAX(CASE
            WHEN dfi.tipo_item = 'COMPONENTE' AND dfi.valor_item > 0 THEN DATE_DIFF(to_date(timestampadd(HOUR, 3, dfi.data_fim_item)),to_date(timestampadd(HOUR, 3, dfi.data_inicio_item)))+1
            ELSE NULL
        END) dias_utilizados_componente,
        con.versao_contrato,
        con.id_contrato,
        con.data_criacao as data_criacao_produto,
        con.produto_nome,
        con.parceiro 
    FROM gold.sydle.dim_faturas_itens dfi
    INNER JOIN gold.sydle.dim_faturas_mailing dfm ON dfi.codigo_fatura_sydle = dfm.codigo_fatura_sydle 
            AND to_date(concat('01/', dfm.mes_referencia), 'dd/MM/yyyy') = trunc(CAST(CURRENT_DATE() AS DATE) - 4, 'MM') 
    INNER JOIN contratos con ON dfm.contrato_air = con.id_contrato
    WHERE (con.produto_nome = TRIM(UPPER(dfi.nome_item)) OR con.item_nome = TRIM(UPPER(dfi.nome_item)))  
    GROUP BY dfi.codigo_fatura_sydle,TRIM(UPPER(dfi.nome_item)), con.versao_contrato, con.id_contrato, con.data_criacao,con.produto_nome,con.parceiro 
)
,faturamento AS (
    SELECT DISTINCT dfm.codigo_fatura_sydle,
        dfm.contrato_air,
        con.codigo_cliente,
        CAST(dfm.data_criacao AS DATE) AS data_criacao_fatura,
        CAST(con.data_ativacao AS DATE) AS data_ativacao,
        CASE
            WHEN con.data_criacao < dfm.data_criacao THEN  DATEDIFF(CAST(dfm.data_criacao AS DATE),CAST(con.data_criacao AS DATE))
            ELSE 999
        END AS diff_ativacao_fatura,
        to_date(concat('01/', dfm.mes_referencia), 'dd/MM/yyyy')  AS mes_referencia,
        dfm.unidade_atendimento,
        dfm.marca,
        dfm.status_fatura,
        fi.nome_item,
        UPPER(con.item_codigo) AS codigo_produto,
        con.produto,
        ROUND(con.valor_item,2) AS valor_produto,
        ROUND(fi.valor_final,2) AS valor_final,        
        fi.dias_utilizados_componente,
        con.cidade,
        con.estado,
        con.produto,
        con.sku,
        con.parceiro
    FROM gold.sydle.dim_faturas_mailing dfm
    INNER JOIN fatura_itens fi ON dfm.codigo_fatura_sydle = fi.codigo_fatura_sydle
    INNER JOIN contratos con ON dfm.contrato_air = con.id_contrato AND fi.versao_contrato = con.versao_contrato AND fi.parceiro = con.parceiro AND fi.produto_nome = con.produto_nome           
    WHERE to_date(concat('01/', dfm.mes_referencia), 'dd/MM/yyyy') = trunc(CAST(CURRENT_DATE() AS DATE) - 4, 'MM')  
        AND dfm.status_fatura IN ('EMITIDA','PAGA','NEGOCIADA COM O CLIENTE') AND fi.dias_utilizados_componente IS NOT NULL
)
,faturamento_filtrado AS (
    SELECT DISTINCT contrato_air,
        codigo_cliente,
        codigo_fatura_sydle,
        data_criacao_fatura,
        data_ativacao,
        mes_referencia,
        unidade_atendimento,
        marca,
        status_fatura,
        nome_item,
        codigo_produto,
        produto,
        sku,
        parceiro,
        valor_produto,
        valor_final,        
        dias_utilizados_componente,
        CASE
          WHEN valor_produto = valor_final THEN "Clientes Fatura Cheia"
          WHEN dias_utilizados_componente >= 30 THEN "Clientes Fatura Cheia"
          WHEN dias_utilizados_componente < 30 THEN "Cliente Pró-Rata"
          ELSE "-"
        END  AS tipo,
        UPPER(TRANSLATE(cidade,"ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ","AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao")) AS cidade,
        UPPER(estado) AS estado, 
        ROW_NUMBER() OVER (PARTITION BY contrato_air,codigo_fatura_sydle, nome_item ORDER BY diff_ativacao_fatura ASC ) as row_num
    FROM faturamento
) 
,ativacoes AS (
  SELECT DISTINCT NULL AS codigo_fatura_sydle,
        con.id_contrato AS contrato_air,
        con.codigo_cliente,
        NULL AS data_criacao_fatura,
        con.data_ativacao,     
        trunc(CAST(CURRENT_DATE() AS DATE) - 4, 'MM')  AS mes_referencia,
        con.unidade AS unidade_atendimento,
        con.marca,
        NULL AS status_fatura,
        con.item_nome AS nome_item,
        con.codigo_produto,
        con.produto,
        con.sku,
        con.parceiro,
        con.valor_item AS valor_produto,
        NULL AS valor_final,        
        DATEDIFF(data_fim,CAST(con.data_ativacao AS DATE)) AS dias_utilizados_componente,
        'Não Faturado' AS tipo,
        UPPER(TRANSLATE(con.cidade,"ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ","AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao")) AS cidade,
        UPPER(con.estado) AS estado,
        versao_contrato
  FROM contratos con
  INNER JOIN (SELECT id_contrato,parceiro,max(versao_contrato) as max_versao
             FROM contratos
             WHERE trunc(CAST(CURRENT_DATE() AS DATE) - 4, 'MM')  >= date_format(data_ativacao, 'yyyy-MM-01') 
             AND  trunc(CAST(CURRENT_DATE() AS DATE) - 4, 'MM')  <= date_format(data_fim, 'yyyy-MM-01')
             AND (data_fim_plano >= CAST(CURRENT_DATE() AS DATE) OR data_fim_plano IS NULL)
             GROUP BY id_contrato,parceiro
  ) max ON con.id_contrato = max.id_contrato and con.versao_contrato = max.max_versao AND con.parceiro = max.parceiro
 
)
, cte_final AS (
    SELECT DISTINCT 
            f.parceiro,
            f.codigo_fatura_sydle,
            f.contrato_air,
            f.codigo_cliente,
            f.data_criacao_fatura,
            f.data_ativacao,
            f.mes_referencia,
            f.unidade_atendimento,
            f.marca,
            f.status_fatura,
            f.nome_item,
            f.codigo_produto,
            f.produto,
            f.sku,
            f.valor_produto,
            f.valor_final,        
            f.dias_utilizados_componente,
            f.tipo,
            f.cidade,
            f.estado,
            COALESCE(sr.macro_regional,'Sem Classificacao') AS macro_regional, 
            COALESCE(sr.sigla_regional,'Sem Classificacao') AS regional, 
            COALESCE(sr.subregional,'Sem Classificacao') AS subregional
    FROM faturamento_filtrado f
    LEFT JOIN silver.stage_seeds_data.subregionais sr ON f.cidade = UPPER(sr.cidade_sem_acento) AND f.estado = sr.uf
    WHERE f.row_num = 1
    UNION ALL 
    SELECT DISTINCT 
            a.parceiro,
            a.codigo_fatura_sydle,
            a.contrato_air,
            a.codigo_cliente,
            a.data_criacao_fatura,
            a.data_ativacao,
            a.mes_referencia,
            a.unidade_atendimento,
            a.marca,
            a.status_fatura,
            a.nome_item,
            a.codigo_produto,
            a.produto,
            a.sku,
            a.valor_produto,
            a.valor_final,        
            a.dias_utilizados_componente,
            a.tipo,
            a.cidade,
            a.estado,
            COALESCE(sr.macro_regional,'Sem Classificacao') AS macro_regional, 
            COALESCE(sr.sigla_regional,'Sem Classificacao') AS regional, 
            COALESCE(sr.subregional,'Sem Classificacao') AS subregional
    FROM ativacoes a
    LEFT JOIN faturamento_filtrado ff ON a.contrato_air = ff.contrato_air AND a.parceiro = ff.parceiro
    LEFT JOIN silver.stage_seeds_data.subregionais sr ON a.cidade = UPPER(sr.cidade_sem_acento) AND a.estado = sr.uf
    WHERE ff.contrato_air IS NULL
)
SELECT      t1.parceiro,
            t1.codigo_fatura_sydle,
            t1.contrato_air,
            t1.codigo_cliente,
            t1.data_criacao_fatura,
            t1.status_fatura,
            t2.status_fatura AS status_fatura_atual,
            t2.data_vencimento as data_vencimento_fatura,
            t2.data_pagamento as data_pagamento_fatura,
            t1.data_ativacao,
            t1.mes_referencia,
            t1.unidade_atendimento,
            t1.marca,            
            t1.nome_item,
            t1.codigo_produto,
            t1.produto,
            t1.sku,
            t1.valor_produto,
            t1.valor_final,        
            t1.dias_utilizados_componente,
            t1.tipo,
            t1.cidade,
            t1.estado,
            t1.macro_regional, 
            t1.regional, 
            t1.subregional,
            CAST(CURRENT_DATE() AS DATE) as data_atualizacao
FROM cte_final t1
LEFT JOIN gold.sydle.dim_faturas_all_versions t2 ON t1.codigo_fatura_sydle = t2.codigo_fatura_sydle AND t2.is_last = true
UNION ALL
SELECT      t1.parceiro,
            t1.codigo_fatura_sydle,
            t1.contrato_air,
            t1.codigo_cliente,
            t1.data_criacao_fatura,
            t1.status_fatura,
            t2.status_fatura AS status_fatura_atual,
            t2.data_vencimento as data_vencimento_fatura,
            t2.data_pagamento as data_pagamento_fatura,
            t1.data_ativacao,
            t1.mes_referencia,
            t1.unidade_atendimento,
            t1.marca,            
            t1.nome_item,
            t1.codigo_produto,
            t1.produto,
            t1.sku,
            t1.valor_produto,
            t1.valor_final,        
            t1.dias_utilizados_componente,
            t1.tipo,
            t1.cidade,
            t1.estado,
            t1.macro_regional, 
            t1.regional, 
            t1.subregional,
            CAST(CURRENT_DATE() AS DATE) as data_atualizacao
FROM {{this}} t1
LEFT JOIN gold.sydle.dim_faturas_all_versions t2 ON t1.codigo_fatura_sydle = t2.codigo_fatura_sydle AND t2.is_last = true
WHERE t1.mes_referencia <> trunc(CAST(CURRENT_DATE() AS DATE) - 4, 'MM') 