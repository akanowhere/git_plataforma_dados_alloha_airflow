-- CTE para assinantes não ativados ou cancelados no Watch
WITH tmp_ott_naoAtivada_Cancelada AS (
    SELECT 'watch' AS provedor,
           A.data_criacao,
           A.data_alteracao,
           A.cliente_id,
           A.contrato_id,
           A.cpf_cnpj,
           A.produto_id,
           A.ativo,
           A.integracao_hub_status,
           A.integracao_transacao
    FROM gold.tv.dim_assinante_watch A
    WHERE A.ativo = false AND A.integracao_hub_status IN ('activate','cancel') AND date_format(A.data_criacao, 'yyyy-MM-01') = date_format(current_date()-1, 'yyyy-MM-01')
    
    UNION ALL 

    -- Assinantes não ativados ou cancelados no YouCast
    SELECT 'youcast' AS provedor,
           A.data_criacao,
           A.data_alteracao,
           A.cliente_id,
           A.contrato_id,
           A.cpf_cnpj,
           A.produto_id,
           A.ativo,
           A.integracao_hub_status,
           A.integracao_transacao
    FROM gold.tv.dim_assinante_youcast A
    WHERE A.ativo = false AND A.integracao_hub_status IN ('activate','cancel') AND date_format(A.data_criacao, 'yyyy-MM-01') = date_format(current_date()-1, 'yyyy-MM-01')

    UNION ALL

    -- Assinantes não ativados ou cancelados no Globoplay
    SELECT 'globoplay' AS provedor,
           A.data_criacao,
           A.data_alteracao,
           A.cliente_id,
           A.contrato_id,
           A.cpf_cnpj,
           A.produto_id,
           A.ativo,
           A.integracao_hub_status,
           A.integracao_transacao
    FROM gold.tv.dim_produto_assinante_globoplay A
    WHERE A.ativo = false AND A.integracao_hub_status IN ('activate','cancel') AND date_format(A.data_criacao, 'yyyy-MM-01') = date_format(current_date()-1, 'yyyy-MM-01')

    UNION ALL

    -- Assinantes não ativados ou cancelados no Skeelo
    SELECT 'skeelo' AS provedor,
           A.data_criacao,
           A.data_alteracao,
           A.cliente_id,
           A.contrato_id,
           A.cpf_cnpj,
           A.produto_id,
           A.ativo,
           A.integracao_hub_status,
           A.integracao_transacao
    FROM gold.tv.dim_assinante_skeelo A
    WHERE A.ativo = false AND A.integracao_hub_status IN ('activate','cancel') AND date_format(A.data_criacao, 'yyyy-MM-01') = date_format(current_date()-1, 'yyyy-MM-01')

    UNION ALL

    -- Assinantes não ativados ou cancelados no Skeelo
    SELECT 'giga+tv' AS provedor,
           A.data_criacao,
           A.data_alteracao,
           A.cliente_id,
           A.contrato_id,
           A.cpf_cnpj,
           A.produto_id,
           A.ativo,
           A.integracao_hub_status,
           A.integracao_transacao
    FROM gold.tv.dim_assinante_gigamais_tv A
    WHERE A.ativo = false AND A.integracao_hub_status IN ('activate','cancel') AND date_format(A.data_criacao, 'yyyy-MM-01') = date_format(current_date()-1, 'yyyy-MM-01')
)

-- Seleção final, juntando os dados de assinantes cancelados e o mapeamento de produtos OTT
SELECT t1.*, CURRENT_DATE() - 1 AS data_referencia
FROM tmp_ott_naoAtivada_Cancelada t1
UNION ALL
SELECT * FROM {{ this }}
WHERE date_format(data_referencia, 'yyyy-MM-01') <> date_format(current_date()-1, 'yyyy-MM-01')
