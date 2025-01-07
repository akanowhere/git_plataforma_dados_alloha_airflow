SELECT
  t1.id AS `ID MIGRACAO`,
  t1.data_venda AS `DATA VENDA`,
  t1.id_venda AS `ID VENDA`,
  t1.id_contrato AS `ID CONTRATO`,
  t1.antigo_pacote_codigo AS `CODIGO ANTIGO PACOTE`,
  t1.antigo_pacote_nome AS `ANTIGO PACOTE NOME`,
  t1.antigo_valor AS `ANTIGO VALOR`,
  t1.antigo_versao AS `ANTIGO VERSAO`,
  t1.novo_pacote_codigo AS `NOVO PACOTE CODIGO`,
  t1.novo_pacote_nome AS `NOVO PACOTE NOME`,
  t1.novo_valor AS `NOVO VALOR`,
  t1.novo_versao AS `NOVO VERSAO`,
  t1.reprovada AS `REPROVADA`,
  t1.data_extracao AS `DATA EXTRACAO`,
  v.email AS `EMAIL_VENDEDOR`,
  u.marca AS `MARCA`,
  v.nome AS `NOME VENDEDOR`
FROM
  {{ get_catalogo('gold') }}.base.dim_migracao t1
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_contrato t2 
         ON t1.id_contrato = t2.id_contrato
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
         ON t2.unidade_atendimento = u.sigla
        AND u.fonte = 'AIR'
  LEFT JOIN {{ get_catalogo('gold') }}.venda.dim_vendedor v
         ON t1.id_vendedor = v.id_vendedor
WHERE
  t1.reprovada = FALSE
  AND t1.data_venda >= '2023-01-01';