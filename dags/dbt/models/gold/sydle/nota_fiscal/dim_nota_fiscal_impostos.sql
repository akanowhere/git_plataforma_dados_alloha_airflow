SELECT
  nota_fiscal.id_nota_fiscal,
  descricao_produto,
  valor_total_dos_impostos_do_produto,
  imposto,
  base_de_calculo,
  aliquota,
  valor,
  retido,
  impostos_retidos,
  exigibilidade,
  data_integracao,
  DATEADD(HOUR, -3, CURRENT_TIMESTAMP()) AS data_extracao
FROM {{ get_catalogo('silver') }}.stage.vw_sydle_tbl_nota_fiscal AS nota_fiscal
LEFT JOIN {{ get_catalogo('silver') }}.stage_sydle.vw_nota_fiscal_impostos AS impostos
  ON nota_fiscal.id_nota_fiscal = impostos.id_nota_fiscal
