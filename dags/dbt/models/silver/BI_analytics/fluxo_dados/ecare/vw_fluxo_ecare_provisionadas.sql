SELECT
  dc.data_criacao AS Data,
  u.nome AS Cidade,
  u.sigla AS Unidade,
  c.onu_serial,
  COUNT(DISTINCT dc.id_contrato) AS Qtd
FROM
  {{ get_catalogo('gold') }}.base.dim_contrato dc
  JOIN {{ get_catalogo('gold') }}.ecare.dim_onu_configuracao oc 
    ON oc.contrato_codigo = dc.id_contrato
  JOIN {{ get_catalogo('gold') }}.base.dim_conexao c 
    ON dc.id_contrato = c.contrato_codigo
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
    ON u.sigla = dc.unidade_atendimento
WHERE
  dc.status NOT IN ('ST_CONT_CANCELADO', 'ST_CONT_EM_ATIVACAO')
  AND oc.status_integracao = 'CONFIGURADA'
  AND OC.vigente = 'true'
GROUP BY
  dc.data_criacao,
  u.nome,
  u.sigla,
  c.onu_serial