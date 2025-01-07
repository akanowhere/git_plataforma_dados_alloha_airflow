 WITH produtos AS (
    SELECT  DISTINCT
            t1.nome as parceiro,
            COALESCE(t5.codigo,t4.codigo,t2.urn) as produto_id,
            t2.urn AS sku,
            t2.descricao AS produto, 
            t3.codigo as codigo_produto   
    FROM gold.tv.dim_sva t1
    INNER JOIN gold.tv.dim_sva_urn t2 ON (t1.id = t2.id_sva)
    INNER JOIN gold.tv.dim_sva_produto_urn t3 ON (t2.id = t3.id_urn)
    LEFT JOIN gold.tv.dim_produtos_watch t4 ON (UPPER(t2.urn) = UPPER(t4.sku))
    LEFT JOIN gold.tv.dim_produtos_gigamaistv t5 ON (UPPER(t2.urn) = UPPER(t5.sku)) 
)  
-- CTE adaptado para Databricks

,cte_ott AS (
    SELECT DISTINCT 'watch' AS provedor, cliente_id, contrato_id, cpf_cnpj, produto_id, ativo, integracao_hub_status, integracao_transacao, integracao_status, data_criacao, data_alteracao
    FROM gold.tv.dim_assinante_watch
    UNION ALL
    SELECT DISTINCT 'youcast' AS provedor, cliente_id, contrato_id, cpf_cnpj, produto_id, ativo, integracao_hub_status, integracao_transacao, integracao_status, data_criacao, data_alteracao
    FROM gold.tv.dim_assinante_youcast
    UNION ALL
    SELECT DISTINCT 'globoplay' AS provedor, cliente_id, contrato_id, cpf_cnpj, produto_id, ativo, integracao_hub_status, integracao_transacao, integracao_status, data_criacao, data_alteracao
    FROM gold.tv.dim_produto_assinante_globoplay
    UNION ALL
    SELECT DISTINCT 'skeelo' AS provedor, cliente_id, contrato_id, cpf_cnpj, produto_id, ativo, integracao_hub_status, integracao_transacao, integracao_status, data_criacao, data_alteracao
    FROM gold.tv.dim_assinante_skeelo
    UNION ALL
    SELECT DISTINCT 'gigatv' AS provedor, cliente_id, contrato_id, cpf_cnpj, produto_id, ativo, integracao_hub_status, integracao_transacao, integracao_status, data_criacao, data_alteracao
    FROM gold.tv.dim_assinante_gigamais_tv
),
cte_contratos AS (
    SELECT 
        t1.codigo_contrato_air AS CONTRATO_CODIGO_AIR,
        t1.data_ativacao_contrato AS DT_ATIVACAO_CONTRATO,
        t2.data_cancelamento AS DT_CANCELAMENTO_CONTRATO,
        t1.data_ativacao_pacote_atual AS DT_ATIVACAO_PACOTE_ATUAL,
        t1.status_contrato AS CONTRATO_STATUS,
        t1.segmento SEGMENTO,
        t1.nome_pacote AS NOME_PACOTE,
        t1.marca AS MARCA,
        TRY_CAST(REPLACE(REPLACE(REPLACE(COALESCE(t5.cpf, t5.cnpj), '.', ''), '-', ''), '/', '') AS BIGINT) AS CPF_CNPJ,
        UPPER(dcp.item_nome) AS ITEM_NOME,
        p.parceiro AS PROVEDOR,
        p.produto_id AS PRODUTO_ID,
        p.sku AS SKU,
        p.produto AS PRODUTO
    FROM gold.relatorios.tbl_basao t1
    LEFT JOIN gold.base.dim_contrato t2 ON t1.codigo_contrato_air = t2.id_contrato    
    LEFT JOIN gold.base.dim_cliente t5 ON t1.codigo_cliente = t5.id_cliente AND t5.fonte = 'AIR'
    LEFT JOIN gold.base.dim_contrato_produto dcp ON t2.id_contrato = dcp.id_contrato AND t2.VERSAO_CONTRATO_AIR = dcp.versao_contrato
    INNER JOIN produtos p ON UPPER(dcp.item_codigo) = UPPER(p.codigo_produto)
    WHERE t1.data_referencia = CURRENT_DATE()-1
)
-- Consulta final
,cte_auditoria AS (
SELECT DISTINCT  
    t1.CONTRATO_CODIGO_AIR,
    t1.DT_ATIVACAO_CONTRATO,
    t1.DT_CANCELAMENTO_CONTRATO,
    t1.DT_ATIVACAO_PACOTE_ATUAL,
    t1.CONTRATO_STATUS,
    t1.SEGMENTO,
    t1.NOME_PACOTE,
    t1.MARCA,
    t1.ITEM_NOME,
    t1.PROVEDOR,
    UPPER(t1.PRODUTO) AS PRODUTO,
    t1.SKU,
    UPPER(t1.PRODUTO_ID) AS PRODUTO_ID,
    t2.ativo AS ATIVO_CRM,
    COALESCE(t2.integracao_hub_status,'-') AS INTEGRACAO_STATUS_HUB,
    COALESCE(t2.integracao_transacao,'-') AS INTEGRACAO_TRANSACAO,
    t2.data_criacao AS DATA_CRIACAO_PRODUTO,
    t2.data_alteracao AS DATA_ALTERACAO_PRODUTO,
    CASE 
        WHEN t2.integracao_hub_status IS NULL AND t2.integracao_transacao IS NULL THEN 1
        WHEN t2.integracao_hub_status = 'activate' AND t2.integracao_transacao in ('activated','reactivated','INTEGRADO') THEN 1
        WHEN t2.integracao_hub_status = 'suspend'	AND t2.integracao_transacao = 'suspended' THEN 1
        WHEN t2.integracao_hub_status = 'cancel'	AND t2.integracao_transacao = 'canceled' THEN 1
        ELSE 0 
    END AS COMPARATIVO_STATUS,
    CURRENT_DATE()-1 AS DATA_REFERENCIA
FROM cte_contratos t1
LEFT JOIN cte_ott t2 ON t1.CONTRATO_CODIGO_AIR = t2.contrato_id AND UPPER(t1.PRODUTO_ID) = UPPER(t2.produto_id)
)
SELECT  CONTRATO_CODIGO_AIR
        ,DT_ATIVACAO_CONTRATO
        ,DT_CANCELAMENTO_CONTRATO
        ,DT_ATIVACAO_PACOTE_ATUAL
        ,CONTRATO_STATUS
        ,SEGMENTO
        ,NOME_PACOTE
        ,MARCA
        ,PROVEDOR 
        ,PRODUTO
        ,ATIVO_CRM
        ,DATA_CRIACAO_PRODUTO
        ,DATA_ALTERACAO_PRODUTO
        ,INTEGRACAO_STATUS_HUB
        ,INTEGRACAO_TRANSACAO
        ,COMPARATIVO_STATUS
        ,DATA_REFERENCIA
FROM cte_auditoria
ORDER BY CONTRATO_CODIGO_AIR DESC, PROVEDOR DESC