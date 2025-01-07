SELECT nota_fiscal.id_nota_fiscal
    ,descricao_produto
    ,CAST(REPLACE(REPLACE(REPLACE(REPLACE((UPPER({{ translate_column(get_catalogo('silver') ~ '.stage_sydle.vw_nota_fiscal_produtos.nome_produto') }})), CHAR(13), ''), CHAR(10), ''), '.', ''), 'º', '') AS VARCHAR(255)) AS nome_produto
    ,valor_base_produto
    ,servico_tributavel_id
    ,CAST(REPLACE(REPLACE(REPLACE((UPPER({{ translate_column(get_catalogo('silver') ~ '.stage_sydle.vw_nota_fiscal_produtos.servico_tributavel_nome') }})), CHAR(13), ''), CHAR(10), ''), '.', '') AS VARCHAR(255)) AS servico_tributavel_nome
    ,valor_total_dos_impostos_do_produto
    ,data_integracao
    ,DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao

FROM {{ get_catalogo('silver') }}.stage.vw_sydle_tbl_nota_fiscal AS nota_fiscal
LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_nota_fiscal_produtos
  ON nota_fiscal.id_nota_fiscal = {{ get_catalogo('silver') }}.stage_sydle.vw_nota_fiscal_produtos.id_nota_fiscal
