select
    CM.id,
    DATE_FORMAT(V.data_venda, 'yyyy-MM-dd HH:mm:ss') AS data_venda,
    id_venda,
    CM.id_contrato,
    CM.antigo_pacote_codigo,
    CM.antigo_pacote_nome,
    CM.antigo_valor,
    CM.antigo_versao,
    CM.novo_pacote_codigo,
    CM.novo_pacote_nome,
    CM.novo_valor,
    CM.novo_versao,
    CM.reprovada,
    NOW() AS data_extracao,
    CAST(V.id_vendedor AS CHAR(256)) AS id_vendedor,
    CAST(U.email AS CHAR(256)) AS email
FROM  {{ get_catalogo('silver') }}.stage.vw_air_tbl_contrato_migracao AS  CM
LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_venda V ON V.id = CM.id_venda
LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_vendedor VE ON VE.id = V.id_vendedor
LEFT JOIN {{ get_catalogo('silver') }}.stage.vw_air_tbl_usuario U ON U.codigo = VE.usuario