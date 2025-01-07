SELECT
  a.id AS id_cliente,
  UPPER(a.cupom) AS cupom
FROM
  {{ get_catalogo('silver') }}.stage.vw_air_tbl_cliente AS a
WHERE
  a.cupom IS NOT NULL
