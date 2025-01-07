SELECT
  pr.id_contrato AS CONTRATO,
  contrato.unidade_atendimento AS UNIDADE,
  pr.servico AS SERVICO,
  pr.primeiro_nivel AS PRIMEIRO_NIVEL,
  pr.segundo_nivel AS SEGUNDO_NIVEL,
  pr.motivo AS MOTIVO,
  pr.submotivo AS SUBMOTIVO,
  pr.observacao AS OBSERVACAO,
  usuario.email AS CRIADOR,
  pr.data_criacao AS DATA_CRIACAO,
  pr.tipo_cliente AS TIPO_CLIENTE
FROM
  {{ get_catalogo('silver') }}.stage.vw_air_tbl_perfilacao_retencao pr
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_usuario usuario 
		ON usuario.codigo = pr.usuario_criacao
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_contrato contrato 
		ON contrato.id_contrato = pr.id_contrato
WHERE
  CAST(pr.data_alteracao AS DATE) >= '2024-01-01'