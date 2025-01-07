SELECT 
    id,
    id_venda,
    CAST(data_criacao AS TIMESTAMP) AS data_criacao,
    CAST(data_alteracao AS TIMESTAMP) AS data_alteracao,
    usuario_criacao,
    usuario_alteracao,
    excluido,
    produto_codigo,
    produto_grupo,
    produto_nome,
    quantidade,
    cast(valor_total AS DECIMAL(10,2)) AS valor_final
FROM silver_dev.stage.vw_air_tbl_venda_adicional