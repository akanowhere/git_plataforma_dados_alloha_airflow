SELECT
  t1.id_contrato,
  t1.id AS id_desconto,
  t1.data_criacao,
  t1.data_alteracao,
  t1.usuario_criacao,
  t1.usuario_alteracao,
  t1.desconto,
  t1.data_validade,
  t1.dia_aplicar,
  t1.item_codigo,
  t1.item_nome,
  t1.tipo,
  t1.categoria,
  t1.observacao,
  t1.data_extracao
FROM
  {{ get_catalogo('gold') }}.base.dim_desconto t1
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato t2 
         ON t1.id_contrato = t2.id_contrato
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
         ON t2.unidade_atendimento = u.sigla
        AND u.fonte = 'AIR'
WHERE
  t1.excluido = FALSE;