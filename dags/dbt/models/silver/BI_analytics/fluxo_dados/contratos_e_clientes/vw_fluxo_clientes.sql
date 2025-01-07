SELECT
  DISTINCT 
  c.id_cliente IdCliente,
  c.nome NomeCliente,
  c.data_criacao DataCriacao,
  c.tipo Tipo,
  c.cpf CPF,
  c.cnpj AS CNPJ,
  c.cupom CUPOM,
  CASE
    WHEN t1.b2b = TRUE THEN 'B2B'
    WHEN t1.pme = TRUE THEN 'PME'
    ELSE 'B2C'
  END SEGMENTO,
  c.fonte_air FONTE,
  CC.nome GRUPOFONTE
FROM
  {{ get_catalogo('gold') }}.base.dim_cliente c
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_catalogo cc ON cc.codigo = c.fonte_air
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato t1 ON c.id_cliente = t1.id_cliente
WHERE
  c.fonte = 'AIR'