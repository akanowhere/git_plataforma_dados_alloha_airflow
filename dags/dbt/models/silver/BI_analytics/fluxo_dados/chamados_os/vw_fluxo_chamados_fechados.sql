SELECT
  c.id AS id_chamado,
  c.data_abertura,
  c.data_transferencia,
  c.usuario_abertura,
  c.data_conclusao,
  c.usuario_atribuido,
  c.codigo_contrato AS id_contrato,
  c.motivo_conclusao,
  c.codigo_cliente AS id_cliente,
  c.endereco_unidade,
  c.contrato_b2b,
  c.prioridade,
  c.data_prioridade,
  c.id_classificacao,
  c.codigo_classificacao,
  c.nome_classificacao,
  c.id_fila,
  c.codigo_fila,
  c.nome_fila,
  f.nome AS nome_fila_origem
FROM
  {{ get_catalogo('gold') }}.chamados.dim_chamado c
  INNER JOIN {{ get_catalogo('gold') }}.base.dim_unidade u 
          ON c.endereco_unidade = u.sigla
         AND u.fonte = 'AIR'
   LEFT JOIN {{ get_catalogo('gold') }}.chamados.dim_chd_transferencia t 
          ON c.id = t.id_chamado
         AND c.data_transferencia = t.data_transferencia
   LEFT JOIN {{ get_catalogo('gold') }}.chamados.dim_fila f
          ON t.id_fila_origem = f.id_fila
WHERE
  c.data_conclusao IS NOT NULL