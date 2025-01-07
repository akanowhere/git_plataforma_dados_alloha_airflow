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
,ativacoes_parceiro AS (
  SELECT  id,
          contrato_id,
          cliente_id,
          data_criacao,
          data_alteracao,
          produto_id,
          integracao_transacao,
          integracao_status,
          integracao_mensagem,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY data_alteracao DESC) AS rn,
         ROW_NUMBER() OVER (PARTITION BY id,REPLACE(integracao_transacao,'reactivated','activated') ORDER BY data_alteracao ASC) AS rn_status
  FROM bronze.air_tv.tbl_produto_assinante_globoplay
  UNION ALL
  SELECT    id,
            contrato_id,
            cliente_id,
            data_criacao,
            data_alteracao,
            produto_id,
            integracao_transacao,
            integracao_status,
            integracao_mensagem,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY data_alteracao DESC) AS rn,
            ROW_NUMBER() OVER (PARTITION BY id,integracao_transacao ORDER BY data_alteracao ASC) AS rn_status
    FROM bronze.air_tv.tbl_assinante_watch
  UNION ALL
  SELECT    id,
            contrato_id,
            cliente_id,
            data_criacao,
            data_alteracao,
            produto_id,
            integracao_transacao,
            integracao_status,
            integracao_mensagem,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY data_alteracao DESC) AS rn,
            ROW_NUMBER() OVER (PARTITION BY id,integracao_transacao ORDER BY data_alteracao ASC) AS rn_status
  FROM bronze.air_tv.tbl_assinante_gigamais_tv  
)
,vendas AS (
  SELECT v.id_venda,
         v.id_contrato AS contrato_air,        
         v.natureza AS tipo_venda,
         v.equipe AS equipe_venda,
         v.data_criacao AS data_venda,
         v.pacote_base_codigo AS codigo_pacote,
         v.pacote_base_nome AS nome_pacote,
         v.valor_total AS valor,
         REPLACE(dc.status,'ST_CONT_','') AS status_contrato,
         dc.id_cliente as codigo_cliente,
         CASE WHEN dc.b2b = TRUE THEN 'B2B' 
            WHEN dc.pme = TRUE THEN 'PME'
            ELSE 'B2C'
         END segmento,
         dc.data_primeira_ativacao AS data_ativacao_contrato,       
         dc.data_cancelamento AS data_cancelamento_contrato,
         dcl.tipo,
         p.parceiro,
         p.produto,
         p.produto_id,
         p.sku,
         p.codigo_produto,
         dci.valor_final AS valor_item,
         UPPER(TRANSLATE(e.cidade,"ÁÉÍÓÚáéíóúÂÊÎÔÛâêîôûÀÈÌÒÙàèìòùÄËÏÖÜäëïöüÇçÑñÃãõ","AEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouCcNnAao")) AS cidade,
         UPPER(e.estado) As estado,
         ap.data_criacao As data_criacao_ott,
         ap.integracao_transacao,
         ap.integracao_status,
         ap.integracao_mensagem,
         ap02.data_alteracao AS data_ativacao_ott,
         ap03.data_alteracao As data_cancelamento_ott

  FROM gold.venda.dim_venda_geral v
  LEFT JOIN gold.base.dim_contrato_produto dcp ON v.id_contrato = dcp.id_contrato AND v.data_criacao = dcp.data_criacao
  INNER JOIN produtos p ON dcp.item_codigo = p.codigo_produto
  LEFT JOIN gold.base.dim_contrato dc ON dcp.id_contrato = dc.id_contrato
  LEFT JOIN gold.base.dim_cliente dcl ON dc.id_cliente = dcl.id_cliente AND dcl.fonte = 'AIR'
  LEFT JOIN gold.base.dim_contrato_item dci ON dcp.id_contrato_produto = dci.id_contrato_produto AND dcp.id_contrato = dci.id_contrato AND dci.excluido = false
  LEFT JOIN gold.base.dim_endereco e ON e.id_endereco = v.id_endereco
  LEFT JOIN ativacoes_parceiro ap ON v.id_contrato = ap.contrato_id AND UPPER(p.produto_id) = UPPER(ap.produto_id) AND ap.rn = 1
  LEFT JOIN ativacoes_parceiro ap02 ON v.id_contrato = ap02.contrato_id AND UPPER(p.produto_id) = UPPER(ap02.produto_id) AND ap02.rn_status = 1 and ap02.integracao_transacao in('activated','reactivated')
  LEFT JOIN ativacoes_parceiro ap03 ON v.id_contrato = ap03.contrato_id AND UPPER(p.produto_id) = UPPER(ap03.produto_id) AND ap03.rn_status = 1 and ap03.integracao_transacao = 'canceled'
  WHERE v.data_criacao >= '2024-09-12' 
        and v.natureza in ('VN_MIGRACAO_UP','VN_NOVO_CONTRATO','VN_MIGRACAO_DOWN','VN_MIGRACAO_COMPULSORIA') 
        and v.cancelada = false 
        and v.excluido = false
)
,ativacoes_ott AS (
    SELECT DISTINCT
         CAST(v.data_venda AS DATE) as data_venda_ott,
         CAST(CASE
            WHEN v.tipo_venda = 'VN_NOVO_CONTRATO' THEN v.data_ativacao_contrato
            ELSE v.data_venda
         END AS DATE) data_ativacao_plano,
         COALESCE(fv.canal_tratado,'MIGRACAO_PLANO') as canal_venda,
         COALESCE(fv.equipe,v.equipe_venda) AS equipe_venda,
         v.id_venda,
         v.contrato_air as contrato,        
         v.codigo_cliente,         
         v.tipo_venda,
         v.codigo_pacote,
         v.nome_pacote,
         v.valor AS valor_plano,
         v.status_contrato AS status_ativacao_bl,
         v.segmento,
         CAST(v.data_ativacao_contrato AS DATE) AS data_ativacao_contrato,       
         CAST(v.data_cancelamento_contrato AS DATE) AS data_cancelamento_contrato,
         v.tipo,
         v.parceiro,
         v.produto,
         v.produto_id,
         v.sku AS sku_ott,
         v.codigo_produto,
         v.valor_item AS valor_ott,       
         v.integracao_transacao AS status_ott,
         CAST(v.data_cancelamento_ott AS DATE) AS data_cancelamento_ott,
         v.data_criacao_ott,
         v.integracao_status,
         v.integracao_mensagem,
         CAST(v.data_ativacao_ott AS DATE) AS data_ativacao_ott,
         v.cidade,
         v.estado,
         COALESCE(s.sigla_regional,'Sem Classificacao') AS regional,	
         COALESCE(s.macro_regional,'Sem Classificacao') AS macro_regional,
         CURRENT_DATE()-1 AS data_referencia
    FROM vendas v
    LEFT JOIN silver.stage_seeds_data.subregionais s ON v.cidade = UPPER(s.cidade_sem_acento) AND v.estado = s.uf
    LEFT JOIN gold.venda.fato_venda fv ON v.contrato_air = fv.id_contrato_air AND v.tipo_venda = 'VN_NOVO_CONTRATO'
)
select * from ativacoes_ott