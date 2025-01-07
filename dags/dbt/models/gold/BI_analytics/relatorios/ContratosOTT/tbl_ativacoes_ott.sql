WITH cte_globoplay AS (
  SELECT  id,
          contrato_id,
          cliente_id,
          data_criacao,
          data_alteracao,
          produto_id,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY data_alteracao ASC) AS rn
  FROM bronze.air_tv.tbl_produto_assinante_globoplay
  WHERE data_criacao >= '2024-09-12' and integracao_transacao = 'activated' 
)
,parceiro AS (
  SELECT  t1.id as id_sva,
          t1.nome as nome_sva,
          t2.id as id_urn,
          t2.urn,
          t2.descricao,
          t3.codigo
  FROM gold.tv.dim_sva t1
  LEFT JOIN gold.tv.dim_sva_urn t2 ON (t1.id = t2.id_sva)
  LEFT JOIN gold.tv.dim_sva_produto_urn t3 ON (t2.id = t3.id_urn)
  WHERe t1.nome = 'GLOBOPLAY'
)
,endereco_entrega AS (
  SELECT id_contrato,
         id_endereco,
         data_criacao,
         COALESCE(LEAD(data_criacao) OVER (PARTITION BY id_contrato ORDER BY data_criacao),CURRENT_DATE()) AS proxima_data
  FROM gold.base.dim_contrato_entrega
)
,vendas AS (
  SELECT v.id_contrato,
         v.id_endereco,
         v.data_criacao,
         COALESCE(LEAD(v.data_criacao) OVER (PARTITION BY v.id_contrato ORDER BY v.data_criacao),CURRENT_TIMESTAMP()) AS proxima_data,
         v.pacote_base_nome,
         v.pacote_base_codigo,
         v.valor_total,
         v.pacote_valor_total,
         v.natureza,
         CASE 
            WHEN natureza = 'VN_NOVO_CONTRATO' THEN 1
            ELSE m.novo_versao 
         END AS versao  
  FROM gold.venda.dim_venda_geral v
  LEFT JOIN gold.base.dim_migracao m ON v.id_contrato = m.id_contrato AND v.id_venda = m.id_venda
  WHERE v.data_criacao >= '2024-09-12' and v.natureza in ('VN_MIGRACAO_UP','VN_NOVO_CONTRATO','VN_MIGRACAO_DOWN','VN_MIGRACAO_COMPULSORIA') and v.cancelada = false and v.excluido = false
)
,ativacoes_ott AS (
    SELECT DISTINCT
    t1.contrato_id AS CONTRATO_CODIGO_AIR,	
    t1.cliente_id AS CODIGO_CLIENTE,	
    CAST(t1.data_alteracao AS DATE) AS DT_ATIVACAO_PRODUTO_GLOBO,	
    CAST(t7.data_criacao AS DATE) AS DT_VENDA,	
    REPLACE(t2.status,'ST_CONT_','') AS CONTRATO_STATUS,	
    t3.tipo AS TIPO_PESSOA,	
    CASE WHEN t2.b2b = TRUE THEN 'B2B' 
        WHEN t2.pme = TRUE THEN 'PME'
        ELSE 'B2C'
    END SEGMENTO,
    t5.estado AS UF,	
    t5.unidade AS UNIDADE,	
    t5.cidade AS CIDADE_TRATADA,
    COALESCE(t6.sigla_regional,'Sem Classificacao') AS REGIONAL,	
    COALESCE(t6.macro_regional,'Sem Classificacao') AS MACRO_REGIONAL,	
    t7.pacote_base_nome AS NOME_PACOTE,
    t7.pacote_base_codigo AS CODIGO_PACOTE,
    t7.valor_total AS VALOR_PACOTE_ATUAL,		
    t1.produto_id AS SKU_PRODUTO_GLOBO,
    t8.descricao AS PRODUTO_GLOBO,
    t8.codigo AS COD_PRODUTO_GLOBO,
    ci.valor_final AS VALOR_PRODUTO_GLOBO
    FROM cte_globoplay t1
    LEFT JOIN gold.base.dim_contrato t2 ON t1.contrato_id = t2.id_contrato
    LEFT JOIN gold.base.dim_cliente t3 ON t1.cliente_id = t3.id_cliente
    LEFT JOIN endereco_entrega t4 ON t1.contrato_id = t4.id_contrato AND t1.data_alteracao BETWEEN t4.data_criacao AND t4.proxima_data
    LEFT JOIN gold.base.dim_endereco t5 ON t4.id_endereco = t5.id_endereco
    LEFT JOIN vendas t7 ON t1.contrato_id = t7.id_contrato AND t1.data_alteracao BETWEEN t7.data_criacao AND t7.proxima_data
    LEFT JOIN parceiro t8 ON UPPER(t1.produto_id) = UPPER(t8.urn)
    INNER JOIN gold.base.dim_contrato_item ci ON t7.id_contrato = ci.id_contrato AND t7.versao = ci.versao_contrato_produto AND UPPER(t8.codigo) = UPPER(ci.item_codigo_produto)
    LEFT JOIN silver.stage_seeds_data.subregionais t6 ON 
    UPPER(
        TRANSLATE(
          t5.cidade,
          "ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ",
          "AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao"
        )
      ) = UPPER(t6.cidade_sem_acento)
    AND t5.estado = t6.uf
    WHERE t1.rn = 1
)
SELECT * FROM ativacoes_ott
