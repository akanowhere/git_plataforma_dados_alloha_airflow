SELECT
  t1.id_cliente AS `CLIENTE CÓDIGO`,
  NULL AS `CLIENTE STATUS`,
  t6.estado AS ESTADO,
  t6.cidade AS CIDADE,
  t6.unidade AS UNIDADE,
  t6.bairro AS BAIRRO,
  t1.id_contrato_air AS `CONTRATO CÓDIGO`,
  NULL AS `CONTRATO CÓDIGO SAP`,
  t1.pacote_codigo AS `PACOTE`,
  t1.pacote_nome AS `PACOTE NOME`,
  t1.valor_final AS `VALOR`,
  t1.valor_final AS VALOR_SEM_DESCONTO,
  COALESCE(t8.nome, t1.status) AS `CONTRATO STATUS`,
  NULL AS `CONTRATO FASE`,
  NULL AS `TIPO TECNOLOGIA`,
  t7.motivo_cancelamento AS `TIPO DE ENCERRAMENTO`,
  t1.data_criacao AS `DATA CRIAÇÃO`,
  t1.data_primeira_ativacao AS `DATA ATIVAÇÃO CONTRATO`,
  t1.data_cancelamento AS `DATA CANCELAMENTO CONTRATO`,
  t1.fechamento_vencimento AS `DIA FECHAMENTO`,
  t1.dia_vencimento AS `DIA VENCIMENTO`,
  NULL AS `EMAIL`,
  NULL AS `CÓD CAMPANHA`,
  NULL AS `CAMPANHA`,
  NULL AS `DATA SUSPENSÃO`,
  t1.marcador AS `MARCADOR`,
  t4.marcador AS `MARCADOR CLIENTE`,
  t1.usuario_criacao AS `USUÁRIO CRIAÇÃO`,
  t2.marca AS `marca`,
  t2.cluster AS CLUSTER,
  NULL AS `CONTRATO_PROPRIO_SUMICITY`,
  t4.cupom AS CUPOM,
  t1.flg_fidelizado AS FIDELIZACAO,
  CASE
    WHEN t1.b2b = TRUE THEN 'B2B'
    WHEN t1.pme = TRUE THEN 'PME'
    ELSE 'B2C'
  END SEGMENTO
FROM
  {{ get_catalogo('gold') }}.base.dim_contrato t1
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_unidade t2 
          ON t1.unidade_atendimento = t2.sigla
         AND t2.fonte = 'AIR'
   LEFT JOIN {{ get_catalogo('gold') }}.base.dim_cliente t4 
          ON t1.id_cliente = t4.id_cliente
         AND t4.fonte = 'AIR'
  LEFT JOIN (
    SELECT
      id_contrato,
      MAX(id_endereco) AS id_endereco
    FROM
      {{ get_catalogo('gold') }}.base.dim_contrato_entrega
    WHERE
      excluido = FALSE
    GROUP BY
      id_contrato
  ) t5 ON t1.id_contrato_air = t5.id_contrato
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_endereco t6 
         ON t5.id_endereco = t6.id_endereco
  LEFT JOIN {{ get_catalogo('gold') }}.base.fato_cancelamento t7 
         ON t1.id_contrato_air = t7.CODIGO_CONTRATO_AIR
  LEFT JOIN {{ get_catalogo('gold') }}.base.dim_catalogo t8 
         ON t1.status = t8.codigo
        AND t8.ativo_catalogo = TRUE
WHERE
  t1.b2b = TRUE