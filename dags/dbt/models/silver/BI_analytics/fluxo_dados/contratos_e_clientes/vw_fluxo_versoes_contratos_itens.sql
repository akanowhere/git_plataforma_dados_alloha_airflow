WITH produtos AS (
    SELECT  DISTINCT
            t1.nome as parceiro,
            t3.codigo as codigo_produto,
            t2.urn AS sku,
            t2.descricao AS sva     
    FROM gold.tv.dim_sva t1
    INNER JOIN gold.tv.dim_sva_urn t2 ON (t1.id = t2.id_sva)
    INNER JOIN gold.tv.dim_sva_produto_urn t3 ON (t2.id = t3.id_urn)
)
SELECT dcp.id_contrato AS contrato_air,
       dcp.versao_contrato,
       CASE
            WHEN dcp.versao_contrato = 1 THEN 'NOVO_CONTRATO'
            ELSE 'MIGRACAO'
       END tipo_versao,
       dcp.data_criacao,
       TRIM(UPPER(dci.item_nome)) AS item_nome,
       dci.valor_final AS valor_item,
       p.sva,
       p.sku
FROM gold.base.dim_contrato_produto dcp 
INNER JOIN gold.base.dim_contrato_item dci ON dcp.id_contrato_produto = dci.id_contrato_produto AND dcp.id_contrato = dci.id_contrato AND dci.excluido = false
LEFT JOIN produtos p ON dcp.item_codigo = p.codigo_produto
WHERE dcp.data_criacao < CURRENT_DATE()
ORDER BY dcp.id_contrato DESC, dcp.versao_contrato DESC;

